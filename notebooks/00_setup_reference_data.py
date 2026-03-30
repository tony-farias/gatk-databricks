# Databricks notebook source
# MAGIC %md
# MAGIC # GATK Reference Data Setup
# MAGIC
# MAGIC Downloads and stages all reference data needed for the GATK best practices
# MAGIC variant calling workflow into Databricks Volumes. This notebook is **idempotent** —
# MAGIC it skips files that already exist.
# MAGIC
# MAGIC **What gets downloaded:**
# MAGIC - hg38 reference genome (FASTA + index + sequence dictionary)
# MAGIC - Known-sites VCFs (dbSNP, Mills & 1000G indels)
# MAGIC - GATK release tarball (extracted so `gatk` CLI is on the volume)
# MAGIC - Sample BAM (NA12878, chr20 subset from 1000 Genomes)

# COMMAND ----------

# Read job parameters passed via base_parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
input_volume = dbutils.widgets.get("input_volume")
output_volume = dbutils.widgets.get("output_volume")
gatk_version = dbutils.widgets.get("gatk_version")

# COMMAND ----------

import subprocess
import os

# Construct volume paths
input_vol_path = f"/Volumes/{catalog}/{schema}/{input_volume}"
output_vol_path = f"/Volumes/{catalog}/{schema}/{output_volume}"

# Subdirectories within the input volume
dirs = {
    "reference": f"{input_vol_path}/reference",
    "known_sites": f"{input_vol_path}/known_sites",
    "bam": f"{input_vol_path}/bam",
    "intervals": f"{input_vol_path}/intervals",
    "intermediate": f"{input_vol_path}/intermediate",
    "tools": f"{input_vol_path}/tools",
}

# Create all subdirectories using dbutils.fs.mkdirs (os.makedirs doesn't work
# with the /Volumes FUSE mount because intermediate paths aren't real directories)
for name, path in dirs.items():
    dbutils.fs.mkdirs(path)
    print(f"Directory ready: {path}")

# Also create intermediate subdirs for the workflow steps
dbutils.fs.mkdirs(f"{dirs['intermediate']}/deduped")
dbutils.fs.mkdirs(f"{dirs['intermediate']}/recalibrated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: idempotent download
# MAGIC Uses `curl` to download files, skipping any that already exist on the volume.

# COMMAND ----------

def download_file(url, dest_path, description=""):
    """Download a file if it doesn't already exist. Prints status either way."""
    if os.path.exists(dest_path):
        print(f"SKIP (exists): {description or dest_path}")
        return
    print(f"Downloading: {description or url}")
    result = subprocess.run(
        ["curl", "-fSL", "--retry", "3", "-o", dest_path, url],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        # Clean up partial download
        if os.path.exists(dest_path):
            os.remove(dest_path)
        raise RuntimeError(f"Download failed for {url}\ncurl exit code: {result.returncode}\nstderr: {result.stderr}")
    print(f"  -> saved to {dest_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reference Genome (hg38)
# MAGIC Downloads the Broad Institute's hg38 reference bundle: FASTA, FASTA index (.fai),
# MAGIC and sequence dictionary (.dict). These are required by every GATK tool.

# COMMAND ----------

# Broad Institute public GCS bucket for hg38 reference files
BROAD_BUCKET = "https://storage.googleapis.com/gcp-public-data--broad-references/hg38/v0"

ref_files = {
    "Homo_sapiens_assembly38.fasta": f"{BROAD_BUCKET}/Homo_sapiens_assembly38.fasta",
    "Homo_sapiens_assembly38.fasta.fai": f"{BROAD_BUCKET}/Homo_sapiens_assembly38.fasta.fai",
    "Homo_sapiens_assembly38.dict": f"{BROAD_BUCKET}/Homo_sapiens_assembly38.dict",
}

for filename, url in ref_files.items():
    download_file(url, f"{dirs['reference']}/{filename}", f"hg38 {filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Known Sites VCFs
# MAGIC dbSNP and Mills & 1000 Genomes indels are used by BQSR (Base Quality Score
# MAGIC Recalibration) to mask known variant sites during recalibration.

# COMMAND ----------

known_sites_files = {
    # dbSNP — used by BQSR and HaplotypeCaller
    "Homo_sapiens_assembly38.dbsnp138.vcf": f"{BROAD_BUCKET}/Homo_sapiens_assembly38.dbsnp138.vcf",
    "Homo_sapiens_assembly38.dbsnp138.vcf.idx": f"{BROAD_BUCKET}/Homo_sapiens_assembly38.dbsnp138.vcf.idx",
    # Mills & 1000 Genomes gold standard indels — used by BQSR
    "Mills_and_1000G_gold_standard.indels.hg38.vcf.gz": f"{BROAD_BUCKET}/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz",
    "Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi": f"{BROAD_BUCKET}/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi",
}

for filename, url in known_sites_files.items():
    download_file(url, f"{dirs['known_sites']}/{filename}", f"Known sites: {filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. GATK Release
# MAGIC Downloads the GATK release tarball from GitHub and extracts it so that the
# MAGIC `gatk` wrapper script is available at a known path on the volume.

# COMMAND ----------

gatk_tarball = f"gatk-{gatk_version}.zip"
gatk_url = f"https://github.com/broadinstitute/gatk/releases/download/{gatk_version}/{gatk_tarball}"
gatk_zip_path = f"{dirs['tools']}/{gatk_tarball}"
gatk_dir = f"{dirs['tools']}/gatk-{gatk_version}"

# Download the zip if the extracted directory doesn't exist
if not os.path.exists(gatk_dir):
    download_file(gatk_url, gatk_zip_path, f"GATK {gatk_version}")
    # Extract
    print(f"Extracting GATK to {dirs['tools']}/")
    result = subprocess.run(
        ["unzip", "-q", "-o", gatk_zip_path, "-d", dirs["tools"]],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Extraction failed: {result.stderr}")
    # Clean up the zip
    os.remove(gatk_zip_path)
    print(f"GATK extracted to {gatk_dir}")
else:
    print(f"SKIP (exists): GATK {gatk_version} already extracted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. GATK Transitive Dependencies
# MAGIC The GATK spark JAR excludes Hadoop and its transitive dependencies. When running
# MAGIC as a JAR task on Databricks, some of these must be provided separately.
# MAGIC We download them to the tools directory so they can be added as task libraries.

# COMMAND ----------

# Dependencies missing from the GATK spark JAR that are needed at runtime.
# These get excluded transitively when Hadoop is removed from the spark configuration.
extra_deps = {
    "grpc-context-1.60.1.jar": "https://repo1.maven.org/maven2/io/grpc/grpc-context/1.60.1/grpc-context-1.60.1.jar",
    "grpc-api-1.60.1.jar": "https://repo1.maven.org/maven2/io/grpc/grpc-api/1.60.1/grpc-api-1.60.1.jar",
}

deps_dir = f"{dirs['tools']}/deps"
dbutils.fs.mkdirs(deps_dir)

for filename, url in extra_deps.items():
    download_file(url, f"{deps_dir}/{filename}", f"Dependency: {filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sample BAM
# MAGIC Downloads a ~20k read subset of NA12878 aligned to hg38 from the GATK test data
# MAGIC bucket. This is a small (~16MB) file suitable for testing the full workflow quickly.

# COMMAND ----------

# GATK test data: NA12878 20k read subset aligned to hg38
SAMPLE_BAM_URL = "https://storage.googleapis.com/gatk-test-data/wgs_bam/NA12878_20k_hg38/NA12878.bam"
SAMPLE_BAI_URL = "https://storage.googleapis.com/gatk-test-data/wgs_bam/NA12878_20k_hg38/NA12878.bai"

download_file(SAMPLE_BAM_URL, f"{dirs['bam']}/NA12878.bam", "Sample BAM: NA12878")
download_file(SAMPLE_BAI_URL, f"{dirs['bam']}/NA12878.bai", "Sample BAM index")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Print a tree of everything staged in the volumes so we can verify at a glance.

# COMMAND ----------

print("=" * 60)
print("GATK Reference Data Setup Complete")
print("=" * 60)

# Walk the input volume and print all files with sizes
for root, _dirs, files in os.walk(input_vol_path):
    level = root.replace(input_vol_path, "").count(os.sep)
    indent = "  " * level
    print(f"{indent}{os.path.basename(root)}/")
    sub_indent = "  " * (level + 1)
    for f in sorted(files):
        size_mb = os.path.getsize(os.path.join(root, f)) / (1024 * 1024)
        print(f"{sub_indent}{f}  ({size_mb:.1f} MB)")

print(f"\nGATK binary: {gatk_dir}/gatk")
print(f"Output volume: {output_vol_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean intermediate and output files
# MAGIC Remove any prior GATK outputs so the downstream JAR tasks always start fresh.

# COMMAND ----------

import glob as globmod
import shutil

# Clean intermediate BAMs from prior runs (may include .parts directories)
for pattern in [
    f"{dirs['intermediate']}/deduped/*",
    f"{dirs['intermediate']}/recalibrated/*",
]:
    for f in globmod.glob(pattern):
        if os.path.isdir(f):
            shutil.rmtree(f)
        else:
            os.remove(f)
        print(f"Cleaned: {f}")

# Clean prior GVCF outputs
for f in globmod.glob(f"{output_vol_path}/*"):
    if os.path.isdir(f):
        shutil.rmtree(f)
    else:
        os.remove(f)
    print(f"Cleaned: {f}")

print("Intermediate and output directories cleaned for fresh run")

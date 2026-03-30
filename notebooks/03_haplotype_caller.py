# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: HaplotypeCaller
# MAGIC
# MAGIC Runs GATK's **HaplotypeCaller** to call germline variants from the recalibrated
# MAGIC BAM file. Outputs a GVCF (genomic VCF) which includes both variant and reference
# MAGIC confidence calls — required for joint genotyping across multiple samples.
# MAGIC
# MAGIC **Input:** Recalibrated BAM from Step 2
# MAGIC **Output:** GVCF in the **output volume** (AutoLoader landing zone for DLT)

# COMMAND ----------

# Read job parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
input_volume = dbutils.widgets.get("input_volume")
output_volume = dbutils.widgets.get("output_volume")
gatk_version = dbutils.widgets.get("gatk_version")

# COMMAND ----------

import subprocess
import os
import glob as globmod

# Volume paths
input_vol = f"/Volumes/{catalog}/{schema}/{input_volume}"
output_vol = f"/Volumes/{catalog}/{schema}/{output_volume}"
gatk_bin = f"{input_vol}/tools/gatk-{gatk_version}/gatk"
subprocess.run(["chmod", "+x", gatk_bin], check=True)

# Input: recalibrated BAM from the previous step
recalibrated_bam = f"{input_vol}/intermediate/recalibrated/NA12878.recalibrated.bam"
assert os.path.exists(recalibrated_bam), f"Recalibrated BAM not found: {recalibrated_bam}"

# Reference genome
ref_fasta = f"{input_vol}/reference/Homo_sapiens_assembly38.fasta"

# Output GVCF goes to the output volume — this is the AutoLoader landing zone
sample_name = "NA12878"
output_gvcf = f"{output_vol}/{sample_name}.g.vcf.gz"

print(f"Input BAM:    {recalibrated_bam}")
print(f"Reference:    {ref_fasta}")
print(f"Output GVCF:  {output_gvcf}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for interval files (optional)
# MAGIC If BED files exist in the `intervals/` directory, pass them to HaplotypeCaller
# MAGIC to restrict calling to specific genomic regions (e.g., exome targets).

# COMMAND ----------

intervals_dir = f"{input_vol}/intervals"
interval_files = globmod.glob(f"{intervals_dir}/*.bed") + globmod.glob(f"{intervals_dir}/*.interval_list")
interval_args = []
for interval_file in interval_files:
    interval_args.extend(["--intervals", interval_file])
    print(f"Using interval file: {interval_file}")

if not interval_args:
    print("No interval files found — calling variants across the whole genome")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run HaplotypeCaller
# MAGIC Uses the standard (non-Spark) HaplotypeCaller which is the production-recommended
# MAGIC tool. For parallelism at scale, run multiple samples concurrently across cluster nodes.

# COMMAND ----------

# Clean prior output
for f in [output_gvcf, f"{output_gvcf}.tbi"]:
    if os.path.exists(f):
        os.remove(f)

cmd = [
    gatk_bin, "HaplotypeCaller",
    "--input", recalibrated_bam,
    "--reference", ref_fasta,
    "--output", output_gvcf,
    "--emit-ref-confidence", "GVCF",
] + interval_args

print(f"Running: {' '.join(cmd)}")
result = subprocess.run(cmd, capture_output=True, text=True)

if result.stdout:
    print(result.stdout[-2000:])
if result.returncode != 0:
    stderr_tail = result.stderr[-3000:] if result.stderr else "(empty)"
    raise RuntimeError(
        f"HaplotypeCaller failed with exit code {result.returncode}\n"
        f"STDERR:\n{stderr_tail}"
    )

print(f"HaplotypeCaller complete: {output_gvcf}")

# COMMAND ----------

# Verify output
if os.path.exists(output_gvcf):
    size_mb = os.path.getsize(output_gvcf) / (1024 * 1024)
    print(f"Output GVCF: {output_gvcf} ({size_mb:.1f} MB)")
    print(f"\nThis GVCF is now in the output volume and will be picked up by AutoLoader")
    print(f"in the DLT pipeline for ingestion into Delta tables.")
else:
    raise FileNotFoundError(f"Expected output not found: {output_gvcf}")

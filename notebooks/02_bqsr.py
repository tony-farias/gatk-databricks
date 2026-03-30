# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Base Quality Score Recalibration (BQSR)
# MAGIC
# MAGIC Runs GATK's **BQSRPipelineSpark** which combines BaseRecalibrator and ApplyBQSR
# MAGIC into a single Spark-optimized step. BQSR corrects systematic errors in base quality
# MAGIC scores assigned by the sequencing machine, using known variant sites as a mask.
# MAGIC
# MAGIC **Input:** Deduplicated BAM from Step 1
# MAGIC **Output:** Recalibrated BAM in `{input_volume}/intermediate/recalibrated/`

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

# Volume paths
input_vol = f"/Volumes/{catalog}/{schema}/{input_volume}"
gatk_bin = f"{input_vol}/tools/gatk-{gatk_version}/gatk"
subprocess.run(["chmod", "+x", gatk_bin], check=True)

# Input: deduplicated BAM from the previous step
deduped_bam = f"{input_vol}/intermediate/deduped/NA12878.deduped.bam"
assert os.path.exists(deduped_bam), f"Deduped BAM not found: {deduped_bam}"

# Reference genome and known sites
ref_fasta = f"{input_vol}/reference/Homo_sapiens_assembly38.fasta"
dbsnp = f"{input_vol}/known_sites/Homo_sapiens_assembly38.dbsnp138.vcf"
mills = f"{input_vol}/known_sites/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz"

# Output path
output_dir = f"{input_vol}/intermediate/recalibrated"
dbutils.fs.mkdirs(output_dir)
recalibrated_bam = f"{output_dir}/NA12878.recalibrated.bam"

print(f"Input BAM:       {deduped_bam}")
print(f"Reference:       {ref_fasta}")
print(f"Known sites:     {dbsnp}, {mills}")
print(f"Output BAM:      {recalibrated_bam}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run BQSRPipelineSpark
# MAGIC This combined tool runs both BaseRecalibrator (builds the recalibration model)
# MAGIC and ApplyBQSR (writes the recalibrated BAM) in a single pass.

# COMMAND ----------

# Clean prior output
for f in [recalibrated_bam, f"{recalibrated_bam}.bai", f"{recalibrated_bam}.sbi"]:
    if os.path.exists(f):
        os.remove(f)

cmd = [
    gatk_bin, "BQSRPipelineSpark",
    "--input", deduped_bam,
    "--reference", ref_fasta,
    "--known-sites", dbsnp,
    "--known-sites", mills,
    "--output", recalibrated_bam,
    "--spark-runner", "LOCAL",
]

print(f"Running: {' '.join(cmd)}")
result = subprocess.run(cmd, capture_output=True, text=True)

if result.stdout:
    print(result.stdout[-2000:])
if result.returncode != 0:
    stderr_tail = result.stderr[-3000:] if result.stderr else "(empty)"
    raise RuntimeError(
        f"BQSRPipelineSpark failed with exit code {result.returncode}\n"
        f"STDERR:\n{stderr_tail}"
    )

print(f"BQSR complete: {recalibrated_bam}")

# COMMAND ----------

# Verify output
if os.path.exists(recalibrated_bam):
    size_mb = os.path.getsize(recalibrated_bam) / (1024 * 1024)
    print(f"Output: {recalibrated_bam} ({size_mb:.1f} MB)")
else:
    raise FileNotFoundError(f"Expected output not found: {recalibrated_bam}")

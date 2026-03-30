# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Mark Duplicates
# MAGIC
# MAGIC Runs GATK's **MarkDuplicatesSpark** to identify and flag duplicate reads in the
# MAGIC input BAM. Duplicate reads arise from PCR amplification during library preparation
# MAGIC and can bias variant calling if not handled.
# MAGIC
# MAGIC **Input:** Raw BAM from `{input_volume}/bam/`
# MAGIC **Output:** Deduplicated BAM in `{input_volume}/intermediate/deduped/`

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
gatk_bin = f"{input_vol}/tools/gatk-{gatk_version}/gatk"

# Ensure gatk wrapper is executable (zip extraction on volumes may not preserve perms)
subprocess.run(["chmod", "+x", gatk_bin], check=True)

# Find the input BAM — expect at least one in the bam/ directory
bam_files = globmod.glob(f"{input_vol}/bam/*.bam")
assert len(bam_files) >= 1, f"No BAM files found in {input_vol}/bam/"
input_bam = bam_files[0]
sample_name = os.path.basename(input_bam).replace(".bam", "")

# Output paths
output_dir = f"{input_vol}/intermediate/deduped"
dbutils.fs.mkdirs(output_dir)
deduped_bam = f"{output_dir}/{sample_name}.deduped.bam"

print(f"Input BAM:  {input_bam}")
print(f"Output BAM: {deduped_bam}")
print(f"GATK:       {gatk_bin}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run MarkDuplicatesSpark
# MAGIC Uses `--spark-runner LOCAL` to run GATK's internal Spark engine on the driver node
# MAGIC using all available cores.

# COMMAND ----------

# Clean prior output
for f in [deduped_bam, f"{deduped_bam}.bai", f"{deduped_bam}.sbi"]:
    if os.path.exists(f):
        os.remove(f)

cmd = [
    gatk_bin, "MarkDuplicatesSpark",
    "--input", input_bam,
    "--output", deduped_bam,
    "--spark-runner", "LOCAL",
    "--metrics-file", f"{output_dir}/{sample_name}.dedup_metrics.txt",
]

print(f"Running: {' '.join(cmd)}")
result = subprocess.run(cmd, capture_output=True, text=True)

if result.stdout:
    print(result.stdout[-2000:])
if result.returncode != 0:
    stderr_tail = result.stderr[-3000:] if result.stderr else "(empty)"
    raise RuntimeError(
        f"MarkDuplicatesSpark failed with exit code {result.returncode}\n"
        f"STDERR:\n{stderr_tail}"
    )

print(f"MarkDuplicates complete: {deduped_bam}")

# COMMAND ----------

# Verify output exists and print size
if os.path.exists(deduped_bam):
    size_mb = os.path.getsize(deduped_bam) / (1024 * 1024)
    print(f"Output: {deduped_bam} ({size_mb:.1f} MB)")
else:
    raise FileNotFoundError(f"Expected output not found: {deduped_bam}")

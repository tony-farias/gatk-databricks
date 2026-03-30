# GATK on Databricks - Implementation Log

This document captures the full implementation journey of running the GATK Best Practices variant calling workflow on Databricks, including every issue encountered and how it was resolved.

## Goal

Run the GATK (Genomics Analysis Toolkit) best practices variant calling workflow entirely on Databricks:
- All inputs (BAMs, reference genome) and outputs (VCF/GVCF) stored in Unity Catalog Volumes
- GATK Spark tools leveraging the cluster's Spark runtime for parallelism
- Output VCFs ingested into Delta tables via AutoLoader and Declarative Pipelines
- Everything deployed as a Databricks Asset Bundle (DAB)

## Architecture

```
Setup (notebook) → MarkDuplicatesSpark → BQSRPipelineSpark → HaplotypeCallerSpark → DLT Ingestion
                   ─────────── JAR tasks (GATK spark JAR) ───────────────          pipeline task
```

Two Unity Catalog volumes:
- `gatk_inputs` — BAMs, reference genome, known sites, GATK binary, intermediate files
- `gatk_outputs` — GVCF landing zone, read by AutoLoader

## Implementation Steps

### Step 1: Initial Scaffold

Created the full DAB project structure:
- `databricks.yml` — bundle config with dev/prod targets
- `resources/variables/vars.yml` — catalog, schema, volume names, GATK version
- `resources/unity_catalog/gatk_setup.yml` — schema + 2 managed volumes
- `resources/gatk_workflow_job.yml` — 5-task chained job
- `resources/vcf_ingestion_pipeline.yml` — serverless DLT pipeline
- `notebooks/00_setup_reference_data.py` — downloads hg38, known sites, GATK, sample BAM
- `notebooks/01_mark_duplicates.py`, `02_bqsr.py`, `03_haplotype_caller.py` — GATK steps via subprocess
- `src/vcf_ingestion.py` — DLT bronze/silver pipeline

Initial approach: notebook tasks that invoke `gatk` CLI via `subprocess` with `--spark-runner SPARK --spark-master local[*]`.

### Step 2: Bundle Validation Issues

**Problem:** `databricks bundle validate` failed — multiple Databricks CLI profiles matched the workspace host.
**Fix:** Pass `--profile e2-demo-field-eng` on the CLI rather than hardcoding in `databricks.yml` (customer won't have our profile).

**Problem:** Warnings about `type: string` in variable definitions.
**Fix:** Removed `type` fields from `vars.yml`. DABs infers type from the default value; `type` only supports `complex`.

### Step 3: Volume Directory Creation (Deploy 1)

**Problem:** `os.makedirs()` failed with `OSError: [Errno 95] Operation not supported` when trying to create subdirectories in `/Volumes/...`.
**Root cause:** The `/Volumes` FUSE mount doesn't support `os.makedirs` for intermediate paths like `/Volumes/catalog/schema` — they're virtual.
**Fix:** Switched to `dbutils.fs.mkdirs()` which handles Volume paths correctly.

### Step 4: Schema Not Found (Deploy 2)

**Problem:** `SCHEMA_NOT_FOUND: zhanf_test.gatk_genomics` — the schema didn't exist at the path the notebooks expected.
**Root cause:** In dev mode, DABs prefixes resource names (e.g., `dev_zach_hanf_gatk_genomics`). The job parameters used `${var.schema_name}` (raw value) instead of the actual deployed resource name.
**Fix:** Changed job parameters to reference resources directly:
```yaml
# Before (wrong in dev mode):
default: ${var.schema_name}
# After (resolves correctly with dev prefix):
default: ${resources.schemas.gatk_schema.name}
```
Applied the same fix to the DLT pipeline's `output_volume_path` configuration.

### Step 5: Download URL Errors (Deploy 3-4)

**Problem 1:** `wget` failed silently with no error message.
**Fix:** Switched from `wget` to `curl -fSL --retry 3` for better error reporting and reliability on Databricks clusters.

**Problem 2:** Reference files returned HTTP 403 from `genomics-public-data` GCS bucket.
**Root cause:** The Broad Institute moved their reference files to a new bucket.
**Fix:** Updated the base URL from:
```
https://storage.googleapis.com/genomics-public-data/resources/broad/hg38/v0
```
to:
```
https://storage.googleapis.com/gcp-public-data--broad-references/hg38/v0
```

**Problem 3:** Sample BAM `NA12878.hg38.snippets.bam` returned 404 — that file doesn't exist in the Broad bucket.
**Fix:** Switched to the GATK test data bucket which has a verified small (~16MB) NA12878 BAM:
```
https://storage.googleapis.com/gatk-test-data/wgs_bam/NA12878_20k_hg38/NA12878.bam
```
Updated all downstream notebook references from `NA12878.hg38.snippets` to `NA12878`.

### Step 6: Java Version Mismatch (Deploy 5)

**Problem:** `UnsupportedClassVersionError: org/broadinstitute/hellbender/Main has been compiled by a more recent version of the Java Runtime (class file version 61.0), this version of the Java Runtime only recognizes class file versions up to 52.0`
**Root cause:** GATK 4.6.1.0 requires Java 17 (class file 61.0). DBR 15.4 ships Java 8 (class file 52.0).
**Fix:** Upgraded cluster from `15.4.x-scala2.12` to `16.3.x-scala2.12` (ships Java 17).

### Step 7: Spark Not Actually Running (Deploy 6)

**Problem:** The GATK steps ran but the Spark UI showed no executor activity — zero active stages, no completed tasks on workers.
**Root cause:** `--spark-master local[*]` forces Spark to run entirely on the driver node. The 4 workers sat idle.
**Attempted fix:** Removed `--spark-master local[*]` to let GATK's `spark-submit` connect to the cluster's actual Spark master.

### Step 8: Long-Running / Hanging Jobs

**Problem:** After removing `local[*]`, we passed the cluster's `spark.master` URL to GATK. But MarkDuplicatesSpark ran for 9+ hours with no progress.
**Root cause:** Running `spark-submit` as a subprocess from a notebook creates a **second** Spark application on the cluster. This causes resource contention with the notebook's own Spark session — the two applications deadlock competing for executor resources.

### Step 9: JAR Task Approach (Attempt 1 — DBR 16.3)

**Idea:** Convert the three GATK steps from notebook tasks to `spark_jar_task` entries, so GATK runs as **the** primary Spark application with full cluster resources.

**Problem:** `An error occurred while initializing the REPL. Please check whether there are conflicting Scala libraries or JARs attached to the cluster, such as Scala 2.11 libraries attached to Scala 2.10 cluster (or vice-versa).`
**Root cause:** GATK 4.6.1.0's `gatk-package-*-spark.jar` is built against **Scala 2.13**, but DBR 16.3 runs **Scala 2.12**. Even though the spark JAR excludes most Spark/Scala deps, some Scala-compiled classes in the JAR conflicted.

### Step 10: Working Baseline with --spark-runner LOCAL

While investigating the Scala mismatch, we ran the workflow successfully using `--spark-runner LOCAL`:
- All 4 GATK tasks passed (setup, mark_duplicates, bqsr, haplotype_caller)
- DLT ingestion created bronze/silver Delta tables
- **Total time: ~48 minutes** (includes cluster startup + reference downloads)
- **Limitation:** All processing on the driver node; workers idle

### Step 11: GATK Source Code Analysis

Cloned the GATK 4.6.1.0 source to understand the build:

```groovy
// build.gradle — key findings:
final sparkVersion = '3.5.0'     // Spark 3.5
// Dependencies use Scala 2.13:
implementation 'org.apache.spark:spark-mllib_2.13:' + sparkVersion

// The sparkConfiguration already excludes Spark/Hadoop/Scala:
sparkConfiguration {
    extendsFrom runtimeClasspath
    exclude group: 'org.apache.hadoop'
    exclude module: 'spark-core_2.13'
    exclude group: 'org.scala-lang'
    // ...
}
```

The `gatk-package-*-spark.jar` is designed to be a thin JAR (no Spark/Hadoop/Scala). The problem was purely the Scala 2.12 vs 2.13 mismatch.

### Step 12: JAR Task Approach (Attempt 2 — DBR 18.0)

**Key discovery:** DBR 18.0 ships with **Scala 2.13**, matching GATK's build target.

Switched cluster to `18.0.x-scala2.13`. The Scala conflict was resolved, but a new error appeared:

**Problem:** `NoClassDefFoundError: io/grpc/Context` — the `grpc-context` library is a transitive dependency that got excluded along with Hadoop.
**Fix:** Added `io.grpc:grpc-context:1.60.1` as a Maven library on each JAR task.

### Current Status

Deployed and running on DBR 18.0 (Scala 2.13) with JAR tasks + gRPC dependency fix. Iterating on any remaining missing transitive dependencies.

## Key Lessons

1. **Volumes FUSE mount** — Use `dbutils.fs.mkdirs()`, not `os.makedirs()`, for creating directories in Unity Catalog Volumes.

2. **DABs dev mode prefixing** — Always reference `${resources.schemas.X.name}` instead of `${var.X}` for names that DABs prefixes in development mode.

3. **GATK + Databricks Spark** — Running `spark-submit` from a notebook subprocess causes resource contention. The correct approach is either:
   - `--spark-runner LOCAL` for single-node execution (simple, reliable)
   - JAR tasks with the thin GATK spark JAR on a Scala-2.13 runtime (distributed)

4. **Scala version alignment** — GATK 4.6.1.0 is built against Scala 2.13. Must use a DBR version with matching Scala (DBR 18.0+).

5. **GATK spark JAR is thin but not thin enough** — The `sparkConfiguration` in GATK's build excludes Spark/Hadoop/Scala, but some transitive dependencies (like `io.grpc:grpc-context`) get caught in the exclusion and must be added back as Maven dependencies.

6. **Reference data URLs** — The Broad Institute moved from `genomics-public-data` to `gcp-public-data--broad-references`. Always verify download URLs before relying on them.

## File Structure

```
gatk/
├── databricks.yml
├── resources/
│   ├── variables/vars.yml
│   ├── unity_catalog/gatk_setup.yml
│   ├── gatk_workflow_job.yml
│   └── vcf_ingestion_pipeline.yml
├── notebooks/
│   ├── 00_setup_reference_data.py      # Downloads + cleanup
│   ├── 01_mark_duplicates.py           # Kept as LOCAL fallback
│   ├── 02_bqsr.py                      # Kept as LOCAL fallback
│   └── 03_haplotype_caller.py          # Kept as LOCAL fallback
└── src/
    └── vcf_ingestion.py                # DLT bronze → silver
```

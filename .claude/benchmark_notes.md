---
name: gatk_benchmark_notes
description: Runtime benchmarks for GATK workflow approaches on Databricks
type: project
---

## GATK Workflow Benchmarks

### Baseline: --spark-runner LOCAL on DBR 16.3 (Scala 2.12)
- **Cluster:** i3.xlarge, 4 workers (not utilized — LOCAL runs on driver only)
- **Sample:** NA12878 20k reads (~16MB BAM)
- **Total job time:** ~48 minutes (includes cluster startup + reference data download)
- **Run URL:** https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/376448879249533/run/76926471877018
- **Date:** 2026-03-20
- **Notes:** Spark UI showed no executor activity — all processing on driver node. Workers were idle.

**Why:** This is our baseline to compare against JAR tasks on DBR 18.0 (Scala 2.13) which should distribute work across workers.

**How to apply:** Compare total job time and Spark UI activity when running the same workflow as JAR tasks on DBR 18.0.

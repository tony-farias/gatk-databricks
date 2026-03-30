# Project Description

## Overview
I have a customer who is interested in running GATK Spark entirely on Databricks. They need all files, inputs, outputs and dependencies to be entirely in Databricks. GATK is a suite of software tools created by the Broad Institute for calling genomic variants given inputs of alignments. In this case, the customer has BAM files that we would like to upload to a Databricks volume. We would then have a job that given that input BAM, a reference genome, and potentially genomic intervals (in bed format) would run the sample through the GATK best practices workflow ( you can find more info about that here:https://github.com/broadinstitute/gatk-docs). The output of these jobs (which should be a vcf/gvcf file, would then go to a landing directory within the volume (or perhaps a separate output volume) that we would then pick up using autloader and lakeflow declarative pipelines as a means to ingest our vcf variants into our lakehouse. We think that we should be able to use the fact that GATK has a spark implementation to be able to parralelize the processing of a single sample using a classic compute cluster on Databricks.

## Technical Implementation
 - Create two volumes: one for inputs (BAMs, reference genome, bed files, known sites VCFs) and one for outputs (final GVCFs, stats files)
 - Create a job that consists of multiple notebook tasks (mapping to the GATK best practices workflow). Each notebook invokes the GATK CLI wrapper via subprocess with `--spark-runner SPARK` to leverage the cluster's Spark for parallelism. We chose notebook tasks over JAR tasks because: (1) the repo has no existing JAR task patterns, (2) notebooks are easier for the customer to debug and modify, (3) GATK Spark tools need careful configuration that's simpler to manage in a notebook, and (4) the `gatk` CLI wrapper handles classpath and system property setup automatically.
 - Land the GVCF outputs into the output volume that we can read with AutoLoader.
 - Create a declarative pipeline to read the output GVCFs (don't care about other outputs for now) into delta tables so that we can query them.

## High-Level Plan

```
gatk/
├── databricks.yml                              # Bundle config (dev/prod targets)
├── resources/
│   ├── variables/vars.yml                      # Catalog, schema, volume names, GATK version
│   ├── unity_catalog/gatk_setup.yml            # Schema + 2 managed volumes
│   ├── gatk_workflow_job.yml                   # 5-task chained job
│   └── vcf_ingestion_pipeline.yml              # Serverless DLT pipeline
├── notebooks/
│   ├── 00_setup_reference_data.py              # Downloads hg38, GATK, known sites, sample BAM to volumes
│   ├── 01_mark_duplicates.py                   # MarkDuplicatesSpark
│   ├── 02_bqsr.py                              # BQSRPipelineSpark (BaseRecalibrator + ApplyBQSR)
│   └── 03_haplotype_caller.py                  # HaplotypeCallerSpark → GVCF to output volume
└── src/
    └── vcf_ingestion.py                        # DLT: AutoLoader → bronze_variants → silver_variants
```

**Job flow**: setup_reference_data → mark_duplicates → bqsr → haplotype_caller → ingest_vcf (DLT pipeline trigger)

All tasks share a classic compute cluster (i3.xlarge, 4 workers). The setup notebook downloads all dependencies (hg38 reference, known-sites VCFs, GATK release, sample BAM) to volumes idempotently. The DLT pipeline ingests the GVCF output into bronze/silver Delta tables using AutoLoader with the VCF schema pattern from the existing genomics example.

## Considerations/Patterns
I don't want the code for this project to be too complicated as a whole. This is something that I will need to hand the customer and have them understand, so please do not be overly complicated when writing code or creating solutions. Please use Databricks Asset Bundles for creating all resources (pipelines, schemas, jobs, volumes) that are required. Please set the default host to: https://e2-demo-field-eng.cloud.databricks.com/. You can find other DABs in this git repo that contain examples for creating DABs. You should also have multiple skills available to you for creating them. Please use declarative pipelines for processing the gvcf data. You can find a simple example of this at: /Users/zach.hanf/Projects/FH/genomics_example. Please use hg38 as a reference genome, and you can find a nice example BAM from the 1K genomes project somewhere on the internet.

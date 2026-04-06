# Deploying GATK on Databricks

Step-by-step guide to deploy the GATK Best Practices variant calling pipeline on Databricks using Asset Bundles.

## Prerequisites

- **Databricks CLI** v0.230+ installed (`brew install databricks` or [install docs](https://docs.databricks.com/dev-tools/cli/install.html))
- **Databricks workspace** with Unity Catalog enabled
- **Cluster access** — a classic compute cluster (GATK Spark tools require JVM-based clusters, not serverless)
- **Unity Catalog permissions** — ability to create catalogs, schemas, and volumes

## 1. Authenticate

```bash
# Login to your workspace
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile my-workspace

# Verify
databricks current-user me --profile my-workspace
```

## 2. Configure Variables

Edit `resources/variables/vars.yml` to match your environment:

```yaml
variables:
  catalog_name:
    default: my_catalog        # Your Unity Catalog catalog
  schema_name:
    default: gatk_genomics     # Schema for GATK tables and volumes
  input_volume_name:
    default: gatk_inputs       # Volume for BAMs, reference genome, known sites
  output_volume_name:
    default: gatk_outputs      # Volume for VCF/GVCF outputs
  gatk_version:
    default: "4.6.1.0"        # GATK version to download
  reference_genome:
    default: hg38              # Reference genome build (hg38 or hg19)
```

Update `databricks.yml` to point to your workspace:

```yaml
targets:
  dev:
    workspace:
      host: https://YOUR-WORKSPACE.cloud.databricks.com
```

## 3. Deploy the Bundle

```bash
# Validate the bundle configuration
databricks bundle validate --profile my-workspace

# Deploy all resources (schemas, volumes, jobs, DLT pipeline)
databricks bundle deploy --profile my-workspace

# This creates:
#   - Unity Catalog schema (gatk_genomics)
#   - Input volume (gatk_inputs)
#   - Output volume (gatk_outputs)
#   - GATK workflow job (4-task chain)
#   - VCF ingestion DLT pipeline
```

## 4. Upload Input Data

Upload your BAM files, reference genome, and known sites to the input volume:

```bash
# Upload a BAM file
databricks fs cp /local/path/sample.bam \
  dbfs:/Volumes/my_catalog/gatk_genomics/gatk_inputs/bams/sample.bam \
  --profile my-workspace

# Upload reference genome (or let notebook 00 download it)
databricks fs cp /local/path/hg38.fa \
  dbfs:/Volumes/my_catalog/gatk_genomics/gatk_inputs/reference/hg38.fa \
  --profile my-workspace
```

Alternatively, run **Notebook 00** to download reference data automatically (see below).

## 5. Run the Pipeline

### Option A: Run via Databricks Jobs (recommended)

```bash
# Run the GATK workflow job
databricks bundle run gatk_workflow_job --profile my-workspace
```

This executes the 4-task chain:
1. **Setup Reference Data** — Downloads reference genome and known sites to the input volume
2. **MarkDuplicates** — Runs GATK MarkDuplicatesSpark on the input BAMs
3. **BQSR** — Runs Base Quality Score Recalibration
4. **HaplotypeCaller** — Runs HaplotypeCallerSpark to produce GVCF files

### Option B: Run Notebooks Individually

You can also run each notebook manually from the Databricks workspace:

| Notebook | Purpose | Input | Output |
|----------|---------|-------|--------|
| `00_setup_reference_data` | Download reference genome + known sites | Internet | `gatk_inputs/reference/` |
| `01_mark_duplicates` | Mark duplicate reads in BAMs | `gatk_inputs/bams/*.bam` | `gatk_outputs/markdup/*.bam` |
| `02_bqsr` | Base Quality Score Recalibration | `gatk_outputs/markdup/*.bam` | `gatk_outputs/bqsr/*.bam` |
| `03_haplotype_caller` | Call variants, produce GVCF | `gatk_outputs/bqsr/*.bam` | `gatk_outputs/gvcf/*.g.vcf` |

## 6. Ingest VCFs into Delta Tables

After the GATK workflow completes, the GVCFs land in the output volume. The DLT pipeline picks them up automatically:

```bash
# Run the VCF ingestion pipeline
databricks bundle run vcf_ingestion_pipeline --profile my-workspace
```

This uses AutoLoader + Lakeflow Declarative Pipelines to:
- Read GVCFs from `gatk_outputs/gvcf/`
- Parse variant records
- Write to Delta tables in `my_catalog.gatk_genomics`

## 7. Query Results

```sql
-- View ingested variants
SELECT * FROM my_catalog.gatk_genomics.variants LIMIT 100;

-- Count variants per chromosome
SELECT chrom, COUNT(*) as variant_count
FROM my_catalog.gatk_genomics.variants
GROUP BY chrom
ORDER BY variant_count DESC;
```

## Cluster Requirements

The GATK Spark tools require a **classic compute cluster** (not serverless):

- **Runtime**: DBR 15.4 LTS or later
- **Node type**: Memory-optimized (e.g., `i3.xlarge` or `r5.xlarge`)
- **Workers**: 2-8 depending on BAM file sizes
- **Spark config**: `spark.master local[*]` for single-node, or default for multi-node
- **Libraries**: GATK is downloaded at runtime by notebook 00; no pre-installed libraries needed

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `bundle validate` fails | Check `databricks.yml` host URL and profile |
| Volume creation fails | Ensure you have `CREATE VOLUME` on the schema |
| GATK download fails | Check internet access from cluster; try manual download to volume |
| MarkDuplicates OOM | Increase driver memory or use a larger node type |
| HaplotypeCaller slow | Add more workers or use intervals (BED file) to parallelize by region |
| DLT pipeline can't read GVCFs | Verify output volume path matches the DLT source config |

## Project Structure

```
gatk-databricks/
├── databricks.yml                          # Bundle config (dev/prod targets)
├── notebooks/
│   ├── 00_setup_reference_data.py          # Download reference genome + known sites
│   ├── 01_mark_duplicates.py               # GATK MarkDuplicatesSpark
│   ├── 02_bqsr.py                          # Base Quality Score Recalibration
│   └── 03_haplotype_caller.py              # HaplotypeCallerSpark → GVCF
├── resources/
│   ├── gatk_workflow_job.yml               # 4-task chained job
│   ├── vcf_ingestion_pipeline.yml          # AutoLoader + DLT pipeline
│   ├── unity_catalog/gatk_setup.yml        # Schema + volume definitions
│   └── variables/vars.yml                  # Configurable variables
└── src/
    └── vcf_ingestion.py                    # VCF → Delta ingestion logic
```

# Databricks notebook source
# MAGIC %md
# MAGIC # VCF Ingestion Pipeline (Spark Declarative Pipeline)
# MAGIC
# MAGIC Ingests GVCF files produced by the GATK workflow into Delta tables using AutoLoader.
# MAGIC
# MAGIC **Bronze (`bronze_variants`):** Raw VCF rows loaded via AutoLoader with a rigid schema.
# MAGIC Includes metadata columns for ingestion tracking.
# MAGIC
# MAGIC **Silver (`silver_variants`):** Cleaned and enriched variants with data quality
# MAGIC expectations applied. Multi-allelic sites are exploded, and FORMAT/SAMPLE fields
# MAGIC are parsed into a queryable map.

# COMMAND ----------

from pyspark import pipelines as dp
import pyspark.sql.functions as fx
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Fetch pipeline configuration — these are passed via the pipeline definition in
# resources/vcf_ingestion_pipeline.yml as Spark conf values.
output_volume_path = spark.conf.get("output_volume_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## VCF Schema
# MAGIC The VCF format is tab-separated with a fixed 10-column structure. We define this
# MAGIC rigidly so any schema deviation from the source files fails the pipeline loudly.

# COMMAND ----------

VCF_SCHEMA = StructType([
    StructField("chr", StringType(), False),
    StructField("pos", IntegerType(), False),
    StructField("id", StringType(), False),
    StructField("ref", StringType(), False),
    StructField("alt", StringType(), False),
    StructField("qual", StringType(), False),
    StructField("filter", StringType(), False),
    StructField("info", StringType(), False),
    StructField("format", StringType(), False),
    StructField("sample_info", StringType(), False),
])

# Columns used to construct a unique variant identifier
variant_id_cols = [fx.col("chr"), fx.col("pos"), fx.col("ref"), fx.col("alt")]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Raw Variant Ingestion
# MAGIC AutoLoader reads `*.g.vcf.gz` files from the output volume. The `comment` option
# MAGIC skips VCF header lines (starting with `#`). Metadata columns track which file
# MAGIC each row came from and when it was ingested.

# COMMAND ----------

@dp.table(
    name="bronze_variants",
    comment="Raw VCF rows ingested via AutoLoader from the GATK output volume"
)
def bronze_variants():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")       # VCFs are tab-separated text
        .option("comment", "#")                    # Skip VCF header lines
        .option("header", False)
        .option("sep", "\t")
        .option("pathGlobfilter", "*.g.vcf.gz")
        .schema(VCF_SCHEMA)                        # Rigid schema — fail on deviation
        .load(output_volume_path)
        # Unique variant identifier: chr-pos-ref-alt
        .withColumn("variant_id", fx.concat_ws("-", *variant_id_cols))
        # AutoLoader _metadata columns for ingestion tracking
        .withColumn("source_file_path", fx.col("_metadata.file_path"))
        .withColumn("source_file_name", fx.col("_metadata.file_name"))
        .withColumn("source_file_size", fx.col("_metadata.file_size"))
        .withColumn("source_file_modification_time", fx.col("_metadata.file_modification_time"))
        .withColumn("ingestion_time", fx.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Cleaned & Enriched Variants
# MAGIC Applies data quality expectations, explodes multi-allelic ALT alleles, and parses
# MAGIC the FORMAT:SAMPLE_INFO fields into a Spark map for easy querying.

# COMMAND ----------

# Data quality expectations — rows that fail these are dropped.
# Adjust thresholds based on your reference genome and use case.
silver_expectations = {
    "chr_is_valid": "chr RLIKE '^chr[0-9XYM]+$'",
    "valid_pos": "pos >= 1",
    "not_non_ref_only": "alt != '<NON_REF>'",
}


@dp.expect_all_or_drop(silver_expectations)
@dp.table(
    name="silver_variants",
    comment="Cleaned variants with quality expectations and parsed sample info"
)
def silver_variants():
    return (
        spark.readStream.table("LIVE.bronze_variants")
        # Parse FORMAT:SAMPLE_INFO into a map, e.g. {"GT": "0/1", "DP": "30", ...}
        .withColumn("split_format", fx.split(fx.col("format"), ":"))
        .withColumn("split_sample_info", fx.split(fx.col("sample_info"), ":"))
        .withColumn("sample_info_map", fx.map_from_arrays(fx.col("split_format"), fx.col("split_sample_info")))
        # Explode multi-allelic ALT alleles into separate rows
        .withColumn("alt", fx.explode(fx.split(fx.col("alt"), ",")))
        .filter(fx.col("alt") != "<NON_REF>")
        # Recompute variant_id after exploding alts
        .withColumn("variant_id", fx.concat_ws("-", *variant_id_cols))
        .drop("split_format", "split_sample_info")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold (future)
# MAGIC A gold table could join these variants with ClinVar annotations, filter on
# MAGIC read depth (DP), or restrict to PASS-only variants. Example:
# MAGIC
# MAGIC ```python
# MAGIC @dp.expect_or_drop("pass_filter", "filter = 'PASS'")
# MAGIC @dp.table(name="gold_clinvar_variants")
# MAGIC def gold_clinvar_variants():
# MAGIC     variants = spark.readStream.table("LIVE.silver_variants")
# MAGIC     clinvar = spark.read.table("your_catalog.clinvar.clinvar_current")
# MAGIC     return (
# MAGIC         variants
# MAGIC         .filter(fx.col("sample_info_map")["DP"].cast("int") > 10)
# MAGIC         .join(clinvar, on="variant_id", how="inner")
# MAGIC     )
# MAGIC ```

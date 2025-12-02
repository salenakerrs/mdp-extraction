"""Spark configs."""
spark_conf_ingestion_pipeline = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.databricks.io.directoryCommit.createSuccessFile": False,
}

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import current_timestamp


def profile_dataframe(df, source_name):
    """
    Profile a Spark DataFrame with basic metrics and data types.
    Returns a DataFrame of profiling metrics.
    """
    spark = df.sparkSession
    row_count = df.count()
    results = []

    for field in df.schema.fields:
        col_name = field.name
        dtype = field.dataType.simpleString()

        null_count = df.filter(F.col(col_name).isNull()).count()
        distinct_count = df.select(col_name).distinct().count()

        metrics = {
            "row_count": row_count,
            "null_count": null_count,
            "null_pct": (null_count / row_count) * 100 if row_count else 0,
            "distinct_count": distinct_count
        }

        for metric, value in metrics.items():
            results.append({
                "source": source_name,
                "column_name": col_name,
                "metric": metric,
                "metric_value": float(value),
                "data_type": dtype
            })

    schema = StructType([
        StructField("source", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("metric_value", DoubleType(), True),
        StructField("data_type", StringType(), False)
    ])

    profile_df = spark.createDataFrame(results, schema=schema) \
        .withColumn("ingestion_timestamp", current_timestamp())

    return profile_df

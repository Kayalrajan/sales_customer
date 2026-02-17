from pyspark.sql import SparkSession

def read_csv(spark, path):
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)

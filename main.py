from pyspark.sql import SparkSession
from extract import read_csv
from transform import transform_sales
from load import write_parquet

spark = SparkSession.builder \
    .appName("Sales ETL Pipeline") \
    .getOrCreate()

sales_df = read_csv(spark, "data/raw/sales.csv")
customers_df = read_csv(spark, "data/raw/customers.csv")

final_df = transform_sales(sales_df, customers_df)

write_parquet(final_df, "data/processed/sales_summary")

spark.stop()

from pyspark.sql.functions import col, sum

def transform_sales(sales_df, customers_df):
    joined_df = sales_df.join(customers_df, "customer_id", "inner")
    result_df = joined_df.groupBy("customer_name") \
                          .agg(sum("amount").alias("total_sales"))
    return result_df

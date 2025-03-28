from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, sum as spark_sum


def top_3_products(spark: SparkSession):
    products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)
    sales_df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
    customers_df = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
    product_quantities_df = (
        sales_df
            .filter((sales_df.sale_date >= "2024-01-01") & (sales_df.sale_date < "2025-01-01"))
            .groupBy(sales_df.product_id)
            .agg(spark_sum(sales_df.quantity).alias("quantity"))
    )
    results_df = (
        sales_df
            .join(products_df, "product_id")
            .join(customers_df, "customer_id")
            .join(product_quantities_df, "product_id")
            .filter(customers_df.country.isin(["Canada", "Mexico", "USA"]))
            .select(
                "country",
                "product_name",
                product_quantities_df.quantity.alias("quantity"),
                rank().over(Window.partitionBy(customers_df.country).orderBy(product_quantities_df.quantity.desc())).alias("rank"),
            )
    )
    results_df = (
        results_df
            .filter(results_df.rank <= 3)
            .orderBy(results_df.country.asc(), results_df.rank.asc())
    )
    results_df.show()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Top3Products").getOrCreate()
    top_3_products(spark)
    spark.stop()

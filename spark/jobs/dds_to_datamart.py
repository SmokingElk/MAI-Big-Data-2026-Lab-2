from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def create_spark_session():
    """Create and configure Spark session with PostgreSQL and ClickHouse connectors"""
    return (SparkSession.builder
            .appName("ETL to ClickHouse")
            .config("spark.jars", "/opt/jars/postgresql.jar,/opt/jars/clickhouse-jdbc.jar")
            .getOrCreate())


def get_postgres_config():
    """Return PostgreSQL connection configuration"""
    return {
        "url": "jdbc:postgresql://postgres:5432/postgres",
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }


def get_clickhouse_config():
    """Return ClickHouse connection configuration"""
    return {
        "url": "jdbc:clickhouse://clickhouse:8123/analytics",
        "user": "default",
        "password": "",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }


def read_table_from_postgres(spark, table_name, pg_config):
    """Read a specific table from PostgreSQL"""
    return (spark.read.format("jdbc")
            .options(**pg_config)
            .option("dbtable", table_name)
            .load())


def write_to_clickhouse(df, table_name, ch_config):
    """Write DataFrame to ClickHouse table"""
    (df.write
     .format("jdbc")
     .options(**ch_config)
     .option("dbtable", table_name)
     .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
     .mode("overwrite")
     .save())


def create_sales_joined_df(spark, pg_config):
    """Create a joined sales dataframe with all dimensions"""
    fact = read_table_from_postgres(spark, "fact_sales", pg_config)
    products = read_table_from_postgres(spark, "dim_products", pg_config)
    customers = read_table_from_postgres(spark, "dim_customers", pg_config)
    stores = read_table_from_postgres(spark, "dim_stores", pg_config)
    suppliers = read_table_from_postgres(spark, "dim_suppliers", pg_config)
    
    return (fact.join(products, on="product_id", how="left")
            .join(customers, on="customer_id", how="left")
            .join(stores, on="store_id", how="left")
            .join(suppliers, on="supplier_id", how="left")
            .withColumn("order_date", F.to_date(F.col("order_date")))
            .withColumn("year", F.year(F.col("order_date")))
            .withColumn("month", F.month(F.col("order_date")))
            .withColumn(
                "customer_name",
                F.concat_ws(" ", F.col("first_name").cast("string"), F.col("last_name").cast("string"))
            )
            .withColumn(
                "supplier_name",
                F.concat_ws(" ", F.col("seller_first_name").cast("string"), F.col("seller_last_name").cast("string"))
            )
            .withColumn("review_count", F.when(F.col("review_id").isNull(), F.lit(0)).otherwise(F.lit(1))))


def create_products_reports(sales_df, ch_config):
    """Create all product-related reports"""
    # Top 10 products by sales
    top_products = (sales_df.groupBy("product_id", "product_name", "product_category")
                    .agg(F.sum("price").alias("total_revenue"),
                         F.count(F.lit(1)).alias("total_sales"))
                    .orderBy(F.col("total_sales").desc(), F.col("total_revenue").desc())
                    .limit(10))
    write_to_clickhouse(top_products, "analytics.report_products_top10", ch_config)
    
    # Revenue by category
    revenue_by_category = (sales_df.groupBy("product_category")
                          .agg(F.sum("price").alias("total_revenue"),
                               F.count(F.lit(1)).alias("total_sales"))
                          .orderBy(F.col("total_revenue").desc()))
    write_to_clickhouse(revenue_by_category, "analytics.report_products_revenue_by_category", ch_config)
    
    # Product ratings and reviews
    product_rating_reviews = (sales_df.groupBy("product_id", "product_name", "product_category")
                              .agg(F.avg("rating").alias("avg_rating"),
                                   F.sum("review_count").cast("long").alias("review_count"))
                              .orderBy(F.col("avg_rating").desc()))
    write_to_clickhouse(product_rating_reviews, "analytics.report_products_rating_reviews", ch_config)


def create_customers_reports(sales_df, ch_config):
    """Create all customer-related reports"""
    customer_spend = (sales_df.groupBy("customer_id", "customer_name", "country")
                     .agg(F.sum("price").alias("total_spent"),
                          F.avg("price").alias("avg_check"),
                          F.count(F.lit(1)).alias("orders_count")))
    
    # Top 10 customers
    top_customers = (customer_spend.orderBy(F.col("total_spent").desc(), F.col("orders_count").desc())
                     .limit(10))
    write_to_clickhouse(top_customers, "analytics.report_customers_top10", ch_config)
    
    # Customers by country
    customers_by_country = (sales_df.groupBy("country")
                           .agg(F.countDistinct("customer_id").alias("customers_count"))
                           .orderBy(F.col("customers_count").desc()))
    write_to_clickhouse(customers_by_country, "analytics.report_customers_by_country", ch_config)
    
    # Average check by customer
    avg_check_by_customer = (customer_spend
                            .select("customer_id", "customer_name", "country", "avg_check", 
                                    "orders_count", "total_spent")
                            .orderBy(F.col("avg_check").desc()))
    write_to_clickhouse(avg_check_by_customer, "analytics.report_customers_avg_check", ch_config)


def create_time_reports(sales_df, ch_config):
    """Create all time-related reports"""
    # Monthly trends
    monthly_trends = (sales_df.groupBy("year", "month")
                     .agg(F.sum("price").alias("total_revenue"),
                          F.count(F.lit(1)).alias("orders_count"))
                     .orderBy("year", "month"))
    write_to_clickhouse(monthly_trends, "analytics.report_time_monthly_trends", ch_config)
    
    # Yearly trends
    yearly_trends = (sales_df.groupBy("year")
                    .agg(F.sum("price").alias("total_revenue"),
                         F.count(F.lit(1)).alias("orders_count"),
                         F.avg("price").alias("avg_order_value"))
                    .orderBy("year"))
    write_to_clickhouse(yearly_trends, "analytics.report_time_yearly_trends", ch_config)
    
    # Average order value by month
    avg_order_by_month = (sales_df.groupBy("year", "month")
                         .agg(F.avg("price").alias("avg_order_value"))
                         .orderBy("year", "month"))
    write_to_clickhouse(avg_order_by_month, "analytics.report_time_avg_order_by_month", ch_config)


def create_stores_reports(sales_df, ch_config):
    """Create all store-related reports"""
    store_base = (sales_df.groupBy("store_id", "store_name", "store_city", "store_state")
                 .agg(F.sum("price").alias("total_revenue"),
                      F.avg("price").alias("avg_check"),
                      F.count(F.lit(1)).alias("orders_count")))
    
    # Top 5 stores by revenue
    top_stores = store_base.orderBy(F.col("total_revenue").desc()).limit(5)
    write_to_clickhouse(top_stores, "analytics.report_stores_top5", ch_config)
    
    # Sales by city/state
    sales_by_city_state = (store_base.groupBy("store_city", "store_state")
                          .agg(F.sum("total_revenue").alias("total_revenue"),
                               F.sum("orders_count").alias("orders_count"),
                               F.avg("avg_check").alias("avg_check"))
                          .orderBy(F.col("total_revenue").desc()))
    write_to_clickhouse(sales_by_city_state, "analytics.report_stores_by_city_state", ch_config)
    
    # Average check by store
    avg_check_by_store = (store_base
                         .select("store_id", "store_name", "store_city", "store_state", 
                                 "avg_check", "orders_count", "total_revenue")
                         .orderBy(F.col("avg_check").desc()))
    write_to_clickhouse(avg_check_by_store, "analytics.report_stores_avg_check", ch_config)


def create_suppliers_reports(sales_df, ch_config):
    """Create all supplier-related reports"""
    supplier_base = (sales_df.groupBy("supplier_id", "supplier_name", "seller_country")
                    .agg(F.sum("price").alias("total_revenue"),
                         F.avg("price").alias("avg_price"),
                         F.count(F.lit(1)).alias("orders_count")))
    
    # Top 5 suppliers by revenue
    top_suppliers = supplier_base.orderBy(F.col("total_revenue").desc()).limit(5)
    write_to_clickhouse(top_suppliers, "analytics.report_suppliers_top5", ch_config)
    
    # Average price by supplier
    avg_price_by_supplier = (supplier_base
                            .select("supplier_id", "supplier_name", "seller_country", 
                                    "avg_price", "orders_count", "total_revenue")
                            .orderBy(F.col("avg_price").desc()))
    write_to_clickhouse(avg_price_by_supplier, "analytics.report_suppliers_avg_price", ch_config)
    
    # Suppliers by country
    suppliers_by_country = (sales_df.groupBy("seller_country")
                           .agg(F.sum("price").alias("total_revenue"),
                                F.countDistinct("supplier_id").alias("suppliers_count"),
                                F.count(F.lit(1)).alias("orders_count"))
                           .orderBy(F.col("total_revenue").desc()))
    write_to_clickhouse(suppliers_by_country, "analytics.report_suppliers_by_country", ch_config)


def create_quality_reports(sales_df, ch_config):
    """Create all product quality reports"""
    product_quality = (sales_df.groupBy("product_id", "product_name", "product_category")
                      .agg(F.avg("rating").alias("avg_rating"),
                           F.count(F.lit(1)).alias("total_sales"),
                           F.sum("review_count").cast("long").alias("review_count")))
    
    # Best and worst products
    best_products = (product_quality
                    .orderBy(F.col("avg_rating").desc(), F.col("total_sales").desc())
                    .limit(10)
                    .withColumn("segment", F.lit("best")))
    
    worst_products = (product_quality
                     .orderBy(F.col("avg_rating").asc(), F.col("total_sales").desc())
                     .limit(10)
                     .withColumn("segment", F.lit("worst")))
    
    best_worst = best_products.unionByName(worst_products)
    write_to_clickhouse(best_worst, "analytics.report_quality_best_worst_products", ch_config)
    
    # Rating-sales correlation
    rating_sales_corr = (product_quality
                        .select(F.corr("avg_rating", "total_sales").alias("rating_sales_correlation")))
    write_to_clickhouse(rating_sales_corr, "analytics.report_quality_rating_sales_correlation", ch_config)
    
    # Most reviewed products
    most_reviews = (product_quality
                   .orderBy(F.col("review_count").desc(), F.col("total_sales").desc())
                   .limit(10))
    write_to_clickhouse(most_reviews, "analytics.report_quality_most_reviews", ch_config)


def main():
    """Main ETL execution function"""
    spark = create_spark_session()
    
    try:
        pg_config = get_postgres_config()
        ch_config = get_clickhouse_config()
        
        # Create joined sales dataframe
        sales_df = create_sales_joined_df(spark, pg_config)
        
        # Generate all reports
        create_products_reports(sales_df, ch_config)
        create_customers_reports(sales_df, ch_config)
        create_time_reports(sales_df, ch_config)
        create_stores_reports(sales_df, ch_config)
        create_suppliers_reports(sales_df, ch_config)
        create_quality_reports(sales_df, ch_config)
        
        print("ETL to ClickHouse completed successfully!")
        
    except Exception as e:
        print(f"Error during ETL process: {e}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
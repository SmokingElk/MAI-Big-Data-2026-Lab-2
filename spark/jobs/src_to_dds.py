from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id


def create_spark_session():
    """Create and configure Spark session with PostgreSQL connector"""
    return (SparkSession.builder
            .appName("ETL to Star Schema")
            .config("spark.jars", "/opt/jars/postgresql.jar")
            .getOrCreate())


def get_db_config():
    """Return PostgreSQL connection configuration"""
    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
    connection_props = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    return jdbc_url, connection_props


def create_product_dimension(df):
    """Create product dimension table"""
    return (df.select(
        col("sale_product_id").alias("product_id"),
        col("product_name"),
        col("product_category"),
        col("product_price")
    ).dropDuplicates(["product_id"]))


def create_customer_dimension(df):
    """Create customer dimension table"""
    return (df.select(
        col("sale_customer_id").alias("customer_id"),
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_country").alias("country")
    ).dropDuplicates(["customer_id"]))


def create_store_dimension(df):
    """Create store dimension table with generated IDs"""
    stores_raw = (df.select(
        col("store_city"),
        col("store_name"),
        col("store_state"),
        col("store_email"),
        col("store_phone")
    ).dropDuplicates())
    
    return stores_raw.withColumn(
        "store_id",
        monotonically_increasing_id()
    )


def create_supplier_dimension(df):
    """Create supplier dimension table with generated IDs"""
    suppliers_raw = (df.select(
        col("seller_first_name"),
        col("seller_last_name"),
        col("seller_email"),
        col("seller_country")
    ).dropDuplicates())
    
    return suppliers_raw.withColumn(
        "supplier_id",
        monotonically_increasing_id()
    )


def create_fact_sales(df, dim_stores, dim_suppliers):
    """Create fact sales table by joining with dimensions"""
    fact_base = (df.select(
        col("sale_product_id").alias("product_id"),
        col("sale_customer_id").alias("customer_id"),
        col("store_city"),
        col("store_name"),
        col("store_state"),
        col("store_email"),
        col("store_phone"),
        col("seller_first_name"),
        col("seller_last_name"),
        col("seller_email"),
        col("seller_country"),
        col("product_price").alias("price"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("review_id"),
        col("sale_date").alias("order_date")
    ))
    
    fact_with_stores = fact_base.join(
        dim_stores,
        on=[
            "store_city",
            "store_name",
            "store_state",
            "store_email",
            "store_phone"
        ],
        how="left"
    )
    
    fact_final = fact_with_stores.join(
        dim_suppliers,
        on=[
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country"
        ],
        how="left"
    )
    
    return fact_final.select(
        "product_id",
        "customer_id",
        "store_id",
        "supplier_id",
        "price",
        "rating",
        "review_id",
        "order_date"
    )


def write_to_postgresql(df, table_name, jdbc_url, props):
    """Write DataFrame to PostgreSQL table"""
    df.write.jdbc(jdbc_url, table_name, "overwrite", props)


def main():
    """Main ETL execution function"""
    spark = create_spark_session()
    
    try:
        jdbc_url, props = get_db_config()
        
        # Read source data
        source_df = spark.read.jdbc(jdbc_url, "mock_data", properties=props)
        
        # Create dimension tables
        dim_products = create_product_dimension(source_df)
        dim_customers = create_customer_dimension(source_df)
        dim_stores = create_store_dimension(source_df)
        dim_suppliers = create_supplier_dimension(source_df)
        
        # Create fact table
        fact_sales = create_fact_sales(source_df, dim_stores, dim_suppliers)
        
        # Write all tables to PostgreSQL
        write_to_postgresql(dim_products, "dim_products", jdbc_url, props)
        write_to_postgresql(dim_customers, "dim_customers", jdbc_url, props)
        write_to_postgresql(dim_stores, "dim_stores", jdbc_url, props)
        write_to_postgresql(dim_suppliers, "dim_suppliers", jdbc_url, props)
        write_to_postgresql(fact_sales, "fact_sales", jdbc_url, props)
        
        print("ETL to Star Schema completed successfully!")
        
    except Exception as e:
        print(f"Error during ETL process: {e}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
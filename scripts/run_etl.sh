#!/bin/bash

set -e  # Exit on any error

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "Err: Docker is not running. Please start Docker first."
        exit 1
    fi
    echo -e "Docker is running"
}

# Check if containers are running
check_containers() {
    local containers=("postgres_bd" "clickhouse_bd" "spark_bd")
    for container in "${containers[@]}"; do
        if ! docker ps | grep -q $container; then
            echo -e "Err: Container $container is not running"
            return 1
        fi
    done
    echo -e "All containers are running"
}

# Start Docker containers
start_containers() {
    echo -e "Starting Docker containers"
    cd docker
    docker-compose up -d
    cd ..
    
    # Wait for containers to be fully ready
    echo -e "Waiting for containers to initialize"
    sleep 20
    
    if ! check_containers; then
        echo -e "Err: Failed to start containers"
        exit 1
    fi
}

# Initialize PostgreSQL database schema
init_postgres_schema() {
    echo -e "Initializing PostgreSQL schema"
    docker exec postgres_bd psql -U postgres -d postgres -c "
        DROP TABLE IF EXISTS fact_sales;
        DROP TABLE IF EXISTS dim_products;
        DROP TABLE IF EXISTS dim_customers;
        DROP TABLE IF EXISTS dim_stores;
        DROP TABLE IF EXISTS dim_suppliers;
        DROP TABLE IF EXISTS mock_data;
    "
    
    # Create tables without constraints for ETL compatibility
    docker exec postgres_bd psql -U postgres -d postgres -c "
        -- Product dimension table
        CREATE TABLE dim_products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(255),
            product_category VARCHAR(100),
            product_price DECIMAL(10, 2)
        );

        -- Customer dimension table
        CREATE TABLE dim_customers (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            country VARCHAR(100)
        );

        -- Store dimension table with auto-generated IDs
        CREATE TABLE dim_stores (
            store_id BIGINT PRIMARY KEY,
            store_name VARCHAR(255),
            store_city VARCHAR(100),
            store_state VARCHAR(100),
            store_email VARCHAR(255),
            store_phone VARCHAR(50)
        );

        -- Supplier dimension table with auto-generated IDs
        CREATE TABLE dim_suppliers (
            supplier_id BIGINT PRIMARY KEY,
            seller_first_name VARCHAR(100),
            seller_last_name VARCHAR(100),
            seller_email VARCHAR(255),
            seller_country VARCHAR(100)
        );

        -- Fact sales table linking all dimensions
        CREATE TABLE fact_sales (
            product_id INT,
            customer_id INT,
            store_id BIGINT,
            supplier_id BIGINT,
            price DECIMAL(10, 2),
            rating DECIMAL(2, 1),
            review_id INT,
            order_date DATE
        );

        -- Add indexes for better query performance
        CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
        CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
        CREATE INDEX idx_fact_sales_date ON fact_sales(order_date);
        CREATE INDEX idx_fact_sales_store ON fact_sales(store_id);
        CREATE INDEX idx_fact_sales_supplier ON fact_sales(supplier_id);
    "
    
    echo -e "PostgreSQL schema initialized"
}

# Load sample data into PostgreSQL
load_sample_data() {
    echo -e "Loading sample data into PostgreSQL"
    
    # Create mock_data table
    docker exec postgres_bd psql -U postgres -d postgres -c "
        CREATE TABLE IF NOT EXISTS mock_data (
            id INT,
            customer_first_name VARCHAR(100),
            customer_last_name VARCHAR(100),
            customer_age INT,
            customer_email VARCHAR(255),
            customer_country VARCHAR(100),
            customer_postal_code VARCHAR(20),
            customer_pet_type VARCHAR(50),
            customer_pet_name VARCHAR(100),
            customer_pet_breed VARCHAR(100),
            seller_first_name VARCHAR(100),
            seller_last_name VARCHAR(100),
            seller_email VARCHAR(255),
            seller_country VARCHAR(100),
            seller_postal_code VARCHAR(20),
            product_name VARCHAR(255),
            product_category VARCHAR(100),
            product_price DECIMAL(10,2),
            product_quantity INT,
            sale_date DATE,
            sale_customer_id INT,
            sale_seller_id INT,
            sale_product_id INT,
            sale_quantity INT,
            sale_total_price DECIMAL(10,2),
            store_name VARCHAR(255),
            store_location VARCHAR(255),
            store_city VARCHAR(100),
            store_state VARCHAR(100),
            store_country VARCHAR(100),
            store_phone VARCHAR(50),
            store_email VARCHAR(255),
            pet_category VARCHAR(100),
            product_weight DECIMAL(10,2),
            product_color VARCHAR(50),
            product_size VARCHAR(50),
            product_brand VARCHAR(100),
            product_material VARCHAR(100),
            product_description TEXT,
            product_rating DECIMAL(2,1),
            product_reviews INT,
            product_release_date DATE,
            product_expiry_date DATE,
            supplier_name VARCHAR(255),
            supplier_contact VARCHAR(255),
            supplier_email VARCHAR(255),
            supplier_phone VARCHAR(50),
            supplier_address TEXT,
            supplier_city VARCHAR(100),
            supplier_country VARCHAR(100)
        );
    "
    
    # Load all CSV files
    for file in data/MOCK_DATA*.csv; do
        if [ -f "$file" ]; then
            echo -e "Loading $file"
            docker exec -i postgres_bd psql -U postgres -d postgres -c "\copy mock_data FROM '$file' WITH CSV HEADER"
        fi
    done
    
    echo -e "Sample data loaded successfully"
}

# Run ETL to Star Schema
run_src_to_dds() {
    echo -e "Running ETL to Star Schema"
    docker exec spark_bd /opt/spark/bin/spark-submit \
        --jars /opt/jars/postgresql.jar \
        /opt/jobs/src_to_dds.py
    
    if [ $? -eq 0 ]; then
        echo -e "ETL to Star Schema completed successfully"
    else
        echo -e "Err: ETL to Star Schema failed"
        exit 1
    fi
}

# Initialize ClickHouse database
init_clickhouse() {
    echo -e "Initializing ClickHouse database"
    docker exec clickhouse_bd clickhouse-client -q "
        CREATE DATABASE IF NOT EXISTS analytics;
    "
    
    echo -e "ClickHouse initialized"
}

# Run ETL to ClickHouse
run_dds_to_datamart() {
    echo -e "Running ETL to ClickHouse"
    docker exec spark_bd /opt/spark/bin/spark-submit \
        --jars /opt/jars/postgresql.jar,/opt/jars/clickhouse-jdbc.jar \
        /opt/jobs/dds_to_datamart.py
    
    if [ $? -eq 0 ]; then
        echo -e "ETL to ClickHouse completed successfully"
    else
        echo -e "Err: ETL to ClickHouse failed"
        exit 1
    fi
}

main() {
    echo -e "ETL Pipeline Execution$"
    
    check_docker
    
    if ! check_containers; then
        echo -e "Containers not running. Starting them"
        start_containers
    fi
    
    init_postgres_schema
    load_sample_data
    run_src_to_dds
    init_clickhouse
    run_dds_to_datamart
    
    echo -e "ETL Pipeline completed successfully"
}

case "${1:-all}" in
    "start")
        start_containers
        ;;
    "all")
        main
        ;;
    "stop")
        cd docker
        docker-compose down
        cd ..
        echo -e "All containers stopped"
        ;;
    *)
        echo "Usage: $0 {start|stop|star|clickhouse|all}"
        echo "  start     - Start Docker containers"
        echo "  stop      - Stop Docker containers"
        echo "  all       - Run complete ETL pipeline"
        exit 1
        ;;
esac
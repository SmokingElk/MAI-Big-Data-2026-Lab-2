#!/bin/bash

echo "ETL Pipeline Validation Results"

print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "ok - $2"
    else
        echo -e "fail - $2"
    fi
}

echo
echo "PostgreSQL Validation:"

# Check mock_data table
mock_count=$(docker exec postgres_bd psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM mock_data" | tr -d ' ')
print_result $? "Mock data loaded ($mock_count rows)"

# Check dimension tables
products_count=$(docker exec postgres_bd psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM dim_products" | tr -d ' ')
print_result $? "Products dimension ($products_count rows)"

customers_count=$(docker exec postgres_bd psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM dim_customers" | tr -d ' ')
print_result $? "Customers dimension ($customers_count rows)"

stores_count=$(docker exec postgres_bd psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM dim_stores" | tr -d ' ')
print_result $? "Stores dimension ($stores_count rows)"

suppliers_count=$(docker exec postgres_bd psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM dim_suppliers" | tr -d ' ')
print_result $? "Suppliers dimension ($suppliers_count rows)"

fact_count=$(docker exec postgres_bd psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM fact_sales" | tr -d ' ')
print_result $? "Fact sales table ($fact_count rows)"

echo "ClickHouse Validation:"

# Check analytics database
db_exists=$(docker exec clickhouse_bd clickhouse-client -q "EXISTS DATABASE analytics" 2>/dev/null)
print_result $? "Analytics database exists"

# Count report tables
table_count=$(docker exec clickhouse_bd clickhouse-client -q "SELECT COUNT(*) FROM system.tables WHERE database = 'analytics'" 2>/dev/null)
print_result $? "Analytics tables created ($table_count/18)"

# Check some key reports
top_products=$(docker exec clickhouse_bd clickhouse-client -q "SELECT COUNT(*) FROM analytics.report_products_top10" 2>/dev/null)
print_result $? "Top 10 products report ($top_products rows)"

top_customers=$(docker exec clickhouse_bd clickhouse-client -q "SELECT COUNT(*) FROM analytics.report_customers_top10" 2>/dev/null)
print_result $? "Top 10 customers report ($top_customers rows)"

top_stores=$(docker exec clickhouse_bd clickhouse-client -q "SELECT COUNT(*) FROM analytics.report_stores_top5" 2>/dev/null)
print_result $? "Top 5 stores report ($top_stores rows)"

monthly_trends=$(docker exec clickhouse_bd clickhouse-client -q "SELECT COUNT(*) FROM analytics.report_time_monthly_trends" 2>/dev/null)
print_result $? "Monthly trends report ($monthly_trends rows)"

echo "Data Quality Checks:"

# Check for null values in critical fields
null_products=$(docker exec postgres_bd psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM fact_sales WHERE product_id IS NULL" | tr -d ' ')
if [ "$null_products" = "0" ]; then
    print_result 0 "No null product IDs in fact table"
else
    print_result 1 "Found $null_products null product IDs"
fi

null_customers=$(docker exec postgres_bd psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM fact_sales WHERE customer_id IS NULL" | tr -d ' ')
if [ "$null_customers" = "0" ]; then
    print_result 0 "No null customer IDs in fact table"
else
    print_result 1 "Found $null_customers null customer IDs"
fi

echo "Sample Data Preview:"

# Show some sample data
echo "Top 3 products by revenue:"
docker exec clickhouse_bd clickhouse-client -q "SELECT product_name, total_revenue FROM analytics.report_products_top10 ORDER BY total_revenue DESC LIMIT 3" --format=Pretty 2>/dev/null

echo "Top 3 customers by spending:"
docker exec clickhouse_bd clickhouse-client -q "SELECT customer_name, total_spent FROM analytics.report_customers_top10 ORDER BY total_spent DESC LIMIT 3" --format=Pretty 2>/dev/null

echo "Monthly sales trends:"
docker exec clickhouse_bd clickhouse-client -q "SELECT year, month, total_revenue FROM analytics.report_time_monthly_trends ORDER BY year, month LIMIT 5" --format=Pretty 2>/dev/null

echo "Validation Complete!"

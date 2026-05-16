#!/usr/bin/env python3
"""
ETL Data Validation Script
This script validates the integrity of the ETL pipeline data
"""

import psycopg2
import clickhouse_driver
from clickhouse_driver import Client as ClickHouseClient
import sys
import os


def get_postgres_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="postgres",
        user="postgres",
        password="postgres"
    )


def get_clickhouse_connection():
    """Get ClickHouse connection"""
    return ClickHouseClient(host="localhost", port=9000)


def print_header(title):
    """Print formatted header"""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def print_result(test_name, passed, details=""):
    """Print validation result"""
    status = "✓ PASS" if passed else "✗ FAIL"
    status_color = "\033[92m" if passed else "\033[91m"  # Green or Red
    reset_color = "\033[0m"
    
    print(f"{status_color}{status}{reset_color} - {test_name}")
    if details:
        print(f"    Details: {details}")


def validate_postgres_schema():
    """Validate PostgreSQL schema and data"""
    print_header("PostgreSQL Schema Validation")
    
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Check if all expected tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = ['dim_products', 'dim_customers', 'dim_stores', 'dim_suppliers', 'fact_sales', 'mock_data']
        
        for table in expected_tables:
            exists = table in tables
            print_result(f"Table {table} exists", exists)
        
        # Check table row counts
        cursor.execute("SELECT COUNT(*) FROM mock_data")
        mock_data_count = cursor.fetchone()[0]
        print_result("Mock data loaded", mock_data_count > 0, f"{mock_data_count} rows")
        
        cursor.execute("SELECT COUNT(*) FROM dim_products")
        products_count = cursor.fetchone()[0]
        print_result("Products dimension", products_count > 0, f"{products_count} unique products")
        
        cursor.execute("SELECT COUNT(*) FROM dim_customers")
        customers_count = cursor.fetchone()[0]
        print_result("Customers dimension", customers_count > 0, f"{customers_count} unique customers")
        
        cursor.execute("SELECT COUNT(*) FROM dim_stores")
        stores_count = cursor.fetchone()[0]
        print_result("Stores dimension", stores_count > 0, f"{stores_count} unique stores")
        
        cursor.execute("SELECT COUNT(*) FROM dim_suppliers")
        suppliers_count = cursor.fetchone()[0]
        print_result("Suppliers dimension", suppliers_count > 0, f"{suppliers_count} unique suppliers")
        
        cursor.execute("SELECT COUNT(*) FROM fact_sales")
        fact_count = cursor.fetchone()[0]
        print_result("Fact sales table", fact_count > 0, f"{fact_count} records")
        
        # Validate referential integrity
        cursor.execute("""
            SELECT COUNT(*) FROM fact_sales fs
            LEFT JOIN dim_products dp ON fs.product_id = dp.product_id
            WHERE dp.product_id IS NULL
        """)
        orphan_products = cursor.fetchone()[0]
        print_result("Fact product FK integrity", orphan_products == 0, 
                    f"{orphan_products} orphan records" if orphan_products > 0 else "All records valid")
        
        cursor.execute("""
            SELECT COUNT(*) FROM fact_sales fs
            LEFT JOIN dim_customers dc ON fs.customer_id = dc.customer_id
            WHERE dc.customer_id IS NULL
        """)
        orphan_customers = cursor.fetchone()[0]
        print_result("Fact customer FK integrity", orphan_customers == 0,
                    f"{orphan_customers} orphan records" if orphan_customers > 0 else "All records valid")
        
        cursor.execute("""
            SELECT COUNT(*) FROM fact_sales fs
            LEFT JOIN dim_stores ds ON fs.store_id = ds.store_id
            WHERE ds.store_id IS NULL
        """)
        orphan_stores = cursor.fetchone()[0]
        print_result("Fact store FK integrity", orphan_stores == 0,
                    f"{orphan_stores} orphan records" if orphan_stores > 0 else "All records valid")
        
        cursor.execute("""
            SELECT COUNT(*) FROM fact_sales fs
            LEFT JOIN dim_suppliers dsup ON fs.supplier_id = dsup.supplier_id
            WHERE dsup.supplier_id IS NULL
        """)
        orphan_suppliers = cursor.fetchone()[0]
        print_result("Fact supplier FK integrity", orphan_suppliers == 0,
                    f"{orphan_suppliers} orphan records" if orphan_suppliers > 0 else "All records valid")
        
        conn.close()
        return True
        
    except Exception as e:
        print_result("PostgreSQL connection", False, str(e))
        return False


def validate_clickhouse_schema():
    """Validate ClickHouse schema and data"""
    print_header("ClickHouse Schema Validation")
    
    try:
        client = get_clickhouse_connection()
        
        # Check if analytics database exists
        databases = client.execute("SHOW DATABASES")
        db_exists = any('analytics' in db for db in databases)
        print_result("Analytics database exists", db_exists)
        
        if not db_exists:
            return False
        
        # Check if tables exist
        tables = client.execute("SHOW TABLES FROM analytics")
        table_names = [table[0] for table in tables]
        
        expected_tables = [
            'analytics.report_products_top10',
            'analytics.report_products_revenue_by_category',
            'analytics.report_products_rating_reviews',
            'analytics.report_customers_top10',
            'analytics.report_customers_by_country',
            'analytics.report_customers_avg_check',
            'analytics.report_time_monthly_trends',
            'analytics.report_time_yearly_trends',
            'analytics.report_time_avg_order_by_month',
            'analytics.report_stores_top5',
            'analytics.report_stores_by_city_state',
            'analytics.report_stores_avg_check',
            'analytics.report_suppliers_top5',
            'analytics.report_suppliers_avg_price',
            'analytics.report_suppliers_by_country',
            'analytics.report_quality_best_worst_products',
            'analytics.report_quality_rating_sales_correlation',
            'analytics.report_quality_most_reviews'
        ]
        
        for table in expected_tables:
            exists = table in table_names
            print_result(f"Table {table.split('.')[1]} exists", exists)
        
        # Check data in key tables
        if 'analytics.report_products_top10' in table_names:
            count = client.execute(f"SELECT COUNT(*) FROM analytics.report_products_top10")[0][0]
            print_result("Top 10 products report", count == 10, f"{count} rows")
        
        if 'analytics.report_customers_top10' in table_names:
            count = client.execute(f"SELECT COUNT(*) FROM analytics.report_customers_top10")[0][0]
            print_result("Top 10 customers report", count == 10, f"{count} rows")
        
        if 'analytics.report_stores_top5' in table_names:
            count = client.execute(f"SELECT COUNT(*) FROM analytics.report_stores_top5")[0][0]
            print_result("Top 5 stores report", count == 5, f"{count} rows")
        
        client.disconnect()
        return True
        
    except Exception as e:
        print_result("ClickHouse connection", False, str(e))
        return False


def validate_data_consistency():
    """Validate data consistency between PostgreSQL and ClickHouse"""
    print_header("Data Consistency Validation")
    
    try:
        pg_conn = get_postgres_connection()
        ch_client = get_clickhouse_connection()
        pg_cursor = pg_conn.cursor()
        
        # Compare total sales counts
        pg_cursor.execute("""
            SELECT COUNT(*), SUM(price) 
            FROM fact_sales
        """)
        pg_sales = pg_cursor.fetchone()
        
        if 'analytics.report_time_yearly_trends' in [t[0] for t in ch_client.execute("SHOW TABLES FROM analytics")]:
            ch_sales = ch_client.execute("""
                SELECT SUM(orders_count), SUM(total_revenue) 
                FROM analytics.report_time_yearly_trends
            """)[0]
            
            count_match = pg_sales[0] == ch_sales[0]
            revenue_match = abs(float(pg_sales[1] or 0) - float(ch_sales[1] or 0)) < 0.01
            
            print_result("Sales count consistency", count_match, 
                        f"PG: {pg_sales[0]}, CH: {ch_sales[0]}")
            print_result("Revenue consistency", revenue_match,
                        f"PG: {pg_sales[1]}, CH: {ch_sales[1]}")
        
        # Compare unique products
        pg_cursor.execute("SELECT COUNT(*) FROM dim_products")
        pg_products = pg_cursor.fetchone()[0]
        
        ch_products = ch_client.execute("""
            SELECT COUNT(DISTINCT product_id) 
            FROM analytics.report_products_revenue_by_category
        """)[0][0] if 'analytics.report_products_revenue_by_category' in [t[0] for t in ch_client.execute("SHOW TABLES FROM analytics")] else 0
        
        print_result("Product count consistency", pg_products == ch_products,
                    f"PG: {pg_products}, CH: {ch_products}")
        
        pg_conn.close()
        ch_client.disconnect()
        return True
        
    except Exception as e:
        print_result("Data consistency check", False, str(e))
        return False


def validate_report_quality():
    """Validate the quality and completeness of reports"""
    print_header("Report Quality Validation")
    
    try:
        ch_client = get_clickhouse_connection()
        
        # Check top products report
        if 'analytics.report_products_top10' in [t[0] for t in ch_client.execute("SHOW TABLES FROM analytics")]:
            top_products = ch_client.execute("""
                SELECT product_name, total_revenue 
                FROM analytics.report_products_top10 
                ORDER BY total_sales DESC, total_revenue DESC 
                LIMIT 5
            """)
            
            print_result("Top products report content", len(top_products) > 0,
                        f"Sample: {top_products[0][0]} - ${top_products[0][1]:.2f}")
        
        # Check quality report
        if 'analytics.report_quality_best_worst_products' in [t[0] for t in ch_client.execute("SHOW TABLES FROM analytics")]:
            quality_data = ch_client.execute("""
                SELECT segment, COUNT(*) 
                FROM analytics.report_quality_best_worst_products 
                GROUP BY segment
            """)
            
            segments = {row[0]: row[1] for row in quality_data}
            has_best = 'best' in segments and segments['best'] == 10
            has_worst = 'worst' in segments and segments['worst'] == 10
            
            print_result("Quality report - best products", has_best)
            print_result("Quality report - worst products", has_worst)
        
        ch_client.disconnect()
        return True
        
    except Exception as e:
        print_result("Report quality check", False, str(e))
        return False


def main():
    """Main validation function"""
    print_header("ETL Pipeline Data Validation")
    
    results = []
    
    # Run all validation checks
    results.append(validate_postgres_schema())
    results.append(validate_clickhouse_schema())
    results.append(validate_data_consistency())
    results.append(validate_report_quality())
    
    print_header("Validation Summary")
    
    passed = sum(results)
    total = len(results)
    
    print(f"\nValidation Tests Completed: {passed}/{total} passed")
    
    if passed == total:
        print("\nAll validation tests passed! The ETL pipeline is working correctly.")
        return 0
    else:
        print(f"\n{total - passed} validation test(s) failed. Please check the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
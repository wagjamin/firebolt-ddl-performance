from firebolt.db import connect
from firebolt.client.auth import ClientCredentials
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import random
import time
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('benchmark.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Service Account credentials
CLIENT_ID = "<add client id here>"
CLIENT_SECRET = "<add client secret here>"
ACCOUNT_NAME = "<add account name here>"

def get_firebolt_connection():
    try:
        connection = connect(
            auth=ClientCredentials(
                CLIENT_ID,
                CLIENT_SECRET
            ),
            account_name=ACCOUNT_NAME
        )
        return connection
    except Exception as e:
        logging.error(f"Failed to create Firebolt connection: {str(e)}")
        raise

def create_single_database(database_index):
    connection = None
    try:
        connection = get_firebolt_connection()
        cursor = connection.cursor()
        database_name = f"test_database_{database_index}"
        
        logging.info(f"Creating database: {database_name}")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        cursor.execute(f"USE DATABASE {database_name}")

        # Check if tables already exist
        cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
        table_count = cursor.fetchone()[0]
        
        if table_count == 100:
            logging.info(f"Database {database_name} already has 100 tables, skipping table creation")
            return True, database_name

        # Set up 100 tables in the database
        logging.info(f"Creating 100 tables in database: {database_name}")
        for j in range(100):
            cursor.execute(f"CREATE TABLE IF NOT EXISTS table_{j} (id INT, name STRING)")
        
        logging.info(f"Successfully created database: {database_name}")
        return True, database_name
        
    except Exception as e:
        logging.error(f"Failed to create database {database_name}: {str(e)}")
        return False, database_name
    finally:
        if connection:
            connection.close()

def create_databases(start_index, count):
    if not all([CLIENT_ID, CLIENT_SECRET, ACCOUNT_NAME]):
        logging.error("Client ID, Client Secret, and Account Name must be provided")
        return

    successful_creates = 0
    failed_creates = 0
    
    logging.info(f"Submitting {count} database creation tasks")
    
    # Use 10 concurrent workers to create the 1000 databases
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_db = {
            executor.submit(create_single_database, i): i 
            for i in range(start_index, start_index + count)
        }
        
        for future in as_completed(future_to_db):
            success, db_name = future.result()
            if success:
                successful_creates += 1
            else:
                failed_creates += 1
            
            logging.info(f"Running totals - Successful: {successful_creates}, Failed: {failed_creates}")
    
    logging.info(f"All creation complete. Final totals - Successful: {successful_creates}, Failed: {failed_creates}")

def run_benchmark(num_databases, num_queries=2, fixed_ddl_database=None):
    connection = None
    try:
        connection = get_firebolt_connection()
        cursor = connection.cursor()
        
        # Create and use the benchmark engine
        logging.info("Creating benchmark engine...")
        cursor.execute("CREATE ENGINE IF NOT EXISTS benchmark_engine")
        cursor.execute("USE ENGINE benchmark_engine")
        logging.info("Benchmark engine created and selected")

        results = []
        for i in range(100):
            # Select random database for SELECT queries
            db_num = random.randint(1, num_databases)
            table_num = random.randint(0, 99)
            database_name = f"test_database_{db_num}"
            
            try:
                # If fixed_ddl_database is set, do DDL there, then switch to random database for SELECT
                if fixed_ddl_database:
                    cursor.execute(f"USE DATABASE {fixed_ddl_database}")
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS ddl_test (id INT, name STRING)")
                    cursor.execute(f"USE DATABASE {database_name}")
                else:
                    # Original behavior: DDL in same database as SELECT
                    cursor.execute(f"USE DATABASE {database_name}")
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS ddl_test (id INT, name STRING)")
                
                # Run and time multiple SELECT queries
                durations = []
                for query_num in range(num_queries):
                    start_time = time.time()
                    cursor.execute(f"SELECT * FROM table_{table_num}")
                    end_time = time.time()
                    duration_ms = (end_time - start_time) * 1000
                    durations.append(duration_ms)
                    
                    # Log each result
                    results.append({
                        'iteration': i,
                        'database': database_name,
                        'is_fixed_ddl': bool(fixed_ddl_database),
                        'table': f"table_{table_num}",
                        'duration_ms': duration_ms,
                        'timestamp': datetime.now().isoformat(),
                        'query_num': query_num
                    })

                # Clean up DDL test table (in the correct database)
                if fixed_ddl_database:
                    cursor.execute(f"USE DATABASE {fixed_ddl_database}")
                cursor.execute(f"DROP TABLE ddl_test")
                
                # Log the iteration results
                duration_str = ", ".join([f"Q{idx}: {dur:.2f}ms" for idx, dur in enumerate(durations)])
                ddl_info = f"(DDL in {fixed_ddl_database})" if fixed_ddl_database else ""
                logging.info(f"Benchmark {i+1}/100: {database_name}.table_{table_num} {ddl_info} - {duration_str}")
                
            except Exception as e:
                logging.error(f"Failed benchmark iteration {i}: {str(e)}")
                continue
        
        # Clean up the engine
        logging.info("Dropping benchmark engine...")
        cursor.execute("USE ENGINE system")
        cursor.execute("STOP ENGINE benchmark_engine")
        cursor.execute("DROP ENGINE benchmark_engine")
        logging.info("Benchmark engine dropped")
        
        return results
        
    except Exception as e:
        logging.error(f"Fatal benchmark error: {str(e)}")
        return []
    finally:
        if connection:
            connection.close()

def create_latency_plot(results, num_databases):
    try:
        # Create DataFrame from results
        df = pd.DataFrame(results)
        
        # Split data by query number
        plot_data = {
            f'Query {i + 1}': df[df['query_num'] == i]['duration_ms']
            for i in range(df['query_num'].max() + 1)
        }
        
        # Create boxplot with high DPI
        plt.figure(figsize=(10, 6), dpi=300)
        plt.boxplot(plot_data.values(), labels=plot_data.keys())
        
        # Use boolean flag directly
        is_fixed_ddl = df.iloc[0]['is_fixed_ddl']
        ddl_type = 'fixed_ddl' if is_fixed_ddl else 'local_ddl'
        title = 'Query Latency Distribution After DDL on Different Database' if is_fixed_ddl else 'Query Latency Distribution After DDL on Same Database'
        
        # Add title and subtitle
        plt.suptitle(title, y=0.95, fontsize=12)
        plt.title(f'({len(df)//len(plot_data)} iterations, uniformly choosing between {num_databases} databases)', 
                 fontsize=10, pad=10)
        
        plt.ylabel('Duration (ms)')
        plt.grid(True)
        
        # Save high-resolution PDF with DDL type in filename
        filename = f'latency_distribution_{num_databases}_dbs_{ddl_type}.pdf'
        plt.savefig(filename, format='pdf', dpi=300, bbox_inches='tight')
        logging.info(f"Created latency distribution plot: {filename}")
        
        # Log additional statistics for each query
        for label, data in plot_data.items():
            stats = data.describe()
            logging.info(f"""
Detailed Statistics ({label}):
-------------------
Count: {stats['count']}
Mean: {stats['mean']:.2f}ms
Std Dev: {stats['std']:.2f}ms
Min: {stats['min']:.2f}ms
25%: {stats['25%']:.2f}ms
50%: {stats['50%']:.2f}ms
75%: {stats['75%']:.2f}ms
Max: {stats['max']:.2f}ms
            """)
        
    except Exception as e:
        logging.error(f"Failed to create latency plot: {str(e)}")

if __name__ == "__main__":
    # First create the databases
    # create_databases(start_index=1, count=1000)
    
    # Then run benchmark with 2 queries per iteration
    logging.info("Starting benchmark...")
    for i in [1, 10, 100, 1000]:
        # Benchmark with picking between i databases at random, running the DDL always on DB 1000
        results = run_benchmark(num_databases=i, num_queries=3, fixed_ddl_database="test_database_1000")
        # Create visualization comparing queries
        create_latency_plot(results, num_databases=i)

        # Benchmark with picking between i databases at random, running the DDL on the same DB we picked
        results = run_benchmark(num_databases=i, num_queries=3, fixed_ddl_database=None)
        # Create visualization comparing queries
        create_latency_plot(results, num_databases=i)

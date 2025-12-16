import pyodbc
import numpy as np
import time
import logging
import pandas as pd
from src.utils import remove_duplicate_cols

# setup loggers
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Load VMware data into database table
def load_vmware_data_into_db(df_vmware, user, password, db_name, host, port, create_table_query, insert_sql_query):
    try:
        start_time = time.time()
        logger.info("Loading vmware data into database initialize.")
        
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={host},{port};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info("Connection established.")

        # Create table
        cursor.execute(create_table_query)
        
        conn.commit()
        logger.info("Table created for VMware.")

        # Replace NaN with None
        df_vmware = df_vmware.where(pd.notnull(df_vmware), None)
        df_vmware = df_vmware.replace({np.nan: None})

        # Convert to list of tuples (each row is a tuple of native Python types)
        data = [tuple(row) for row in df_vmware.itertuples(index=False, name=None)]
        
        # Batch insert
        cursor.fast_executemany = True
        cursor.executemany(insert_sql_query, data)
        conn.commit()
        logger.info("Batch insert completed.")

    except Exception as e:
        logger.error("Error while loading data for VirtualMachine into database table:", e)

    finally:
        end_time = time.time() -start_time
        logger.info(f'Time taken to complete data loading: {end_time}')
        try:
            cursor.close()
            conn.close()
            print(" Connection closed.")
        except:
            pass


# Load AMPs data into database table
def load_amps_data_into_db(df_view, view_name, user, password, db_name, host, port):
    start_time = time.time()
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info("Database Connection established.")
    
        # We are not passing create_table_query and insert_sql_query as arguments because the DataFrame contains too many columns.
        # Instead, we use a script that dynamically generates the CREATE TABLE and INSERT statements by inspecting the DataFrame structure.

        # remove duplicate columns before creating table
        remove_duplicate_cols(df_view)
        
        # Generate CREATE TABLE statement
        table_name = view_name
        columns = df_view.columns
        sql_types = {
            "object": "NVARCHAR(MAX)",
            "float64": "FLOAT",
            "int64": "INT",
            "bool": "BIT",
            "datetime64[ns]": "DATETIME"
        }
    
        create_stmt = f"IF OBJECT_ID('dbo.{table_name}', 'U') IS NOT NULL DROP TABLE dbo.{table_name};\nCREATE TABLE dbo.{table_name} (\n"
        
        # Creating create table statement for each 
        for col in columns:
            dtype = str(df_view[col].dtype)
            sql_type = sql_types.get(dtype, "NVARCHAR(MAX)")
            create_stmt += f"    [{col}] {sql_type},\n"
        create_stmt = create_stmt.rstrip(",\n") + "\n);"
    
        # Create table
        cursor.execute(create_stmt)
        conn.commit()
        logger.info("Table created.")
    
        # Replace NaN with None
        df_view = df_view.where(pd.notnull(df_view), None)
        df_view = df_view.replace({np.nan: None})
    
        
        # Convert to list of tuples (each row is a tuple of native Python types)
        data = [tuple(row) for row in df_view.itertuples(index=False, name=None)]
    
        
        # Prepare insert statement
        placeholders = ",".join(["?"] * len(columns))
        insert_sql = f"INSERT INTO dbo.{table_name} VALUES ({placeholders})"
    
        
        # Batch insert
        cursor.fast_executemany = True
        
        chunk_size = 1000  # You can adjust this based on available memory
        for i in range(0, len(data), chunk_size):
            logger.info(f'range: {i}')
            chunk = data[i:i+chunk_size]
            cursor.executemany(insert_sql, chunk)
            conn.commit()
    
        # cursor.executemany(insert_sql, data)
        # conn.commit()
        logger.info("Batch insert completed.")
    
    except Exception as e:
        logger.info("Error:", e)
        
    
    finally:
        end_time = time.time() -start_time
        logger.info(f'Time taken to complete data loading: {end_time}')
        try:
            cursor.close()
            conn.close()
            logger.info(" Connection closed.")
        except:
            pass


# Run Custom Query database table
def run_custom_query(query, user, password, db_name, host, port):
    
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info("Database Connection established to run custom query.")
    
        # Run query
        cursor.execute(query)
        conn.commit()

    
        logger.info("Query completed.")
    
    except Exception as e:
        logger.info("Error:", e)
        
    
    finally:
        try:
            cursor.close()
            conn.close()
            logger.info(" Connection closed.")
        except:
            pass

# Query to create indexes
# this will be used for creating the index for the columns as our most of the columns having Max length
## and we can not create the index for a column with Max length
def create_index(table, column, user, password, db_name, host, port):
    
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info(f"Database Connection established to create index for {table}.{column}")
    
        # get the max lenght of column
        len_query = f"""
                SELECT MAX(LEN([{column}])) AS MaxLength
                    FROM dbo.{table};
                """
        
        cursor.execute(len_query)
        max_len = cursor.fetchall()[0][0] if cursor.fetchall()[0][0] > 255 else 255

        # Alter  column with change in its length
        alter_query = f"""
        ALTER TABLE dbo.{table}
        ALTER COLUMN [{column}] VARCHAR({max_len});
        """
        cursor.execute(alter_query)

        # create index query
        create_index_query = f"""
                CREATE INDEX IX_{table}_{column} ON EOSLdatastore.dbo.{table}([{column}]);
        """
        cursor.execute(create_index_query)
        
        conn.commit()

    
        logger.info("Query completed.")
    
    except Exception as e:
        logger.info("Error:", e)
        
    
    finally:
        try:
            cursor.close()
            conn.close()
            logger.info(" Connection closed.")
        except:
            pass
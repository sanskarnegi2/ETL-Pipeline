import pyodbc
import numpy as np
import time
import logging
import pandas as pd
import re
from src.utils import remove_duplicate_cols, send_failure_email, normalize_column
from sqlalchemy import create_engine

# setup loggers
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_sql_server_connection(user, password, db_name, host, port=None, driver='ODBC Driver 17 for SQL Server', timeout=60, retries=3, retry_delay=5):
    server = f"{host},{port}" if port else host
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={db_name};"
        f"UID={user};"
        f"PWD={password};"
        f"Connection Timeout={timeout};"
        f"LoginTimeout={timeout};"
    )
    last_exception = None
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Connecting to SQL Server {server} (attempt {attempt}/{retries})")
            return pyodbc.connect(conn_str)
        except Exception as exc:
            last_exception = exc
            logger.warning(f"SQL Server connection attempt {attempt} failed: {exc}")
            if attempt < retries:
                time.sleep(retry_delay)
    raise last_exception


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
        send_failure_email('load_vmware_data_into_db', 'Something went wrong while loading VMware data into the database.', e)

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
        conn = get_sql_server_connection(user, password, db_name, host, port)
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
        send_failure_email('load_amps_data_into_db', 'Something went wrong while loading AMPs data into the database.', e)
        
    
    finally:
        end_time = time.time() -start_time
        logger.info(f'Time taken to complete data loading: {end_time}')
        try:
            cursor.close()
            conn.close()
            logger.info(" Connection closed.")
        except:
            pass


# Load other data into database table
def load_data_into_db(df_view, view_name, user, password, db_name, host, port):
    start_time = time.time()
    try:
        # Connect to SQL Server
        conn = get_sql_server_connection(user, password, db_name, host, port)
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
        send_failure_email('load_data_into_db', 'Something went wrong while loading data into the database.', e)
        
    
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
        send_failure_email('run_custom_query', 'Something went wrong while running custom query.', e)
        
    
    finally:
        try:
            cursor.close()
            conn.close()
            logger.info(" Connection closed.")
        except:
            pass

# load new DDBoost clients into database table
def load_ddboost_clients_into_db(df_clients, user, password, db_name, host, port):
    try:
        
        # create sqlalchemy engine
        engine = create_engine(
            f"mssql+pyodbc://{user}:{password}@{host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
        )

        # Load data into SQL Server using to_sql(append)
        df_clients.to_sql(
                        "ddboost_clients",
                        engine,
                        if_exists="append",
                        index=False
                    )
    
        logger.info("Query completed.")
    
    except Exception as e:
        logger.info("Error:", e)
        send_failure_email('load_ddboost_clients_into_db', 'Something went wrong while loading DDBoost clients into the database.', e)
        
    
    




# Function to fetch table data from datbase 
def fetch_table_data(query, user, password, db_name, host, port):
    
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info("Database Connection established to fetch table data.")
    
        # Run SELECT query
        #"SELECT * FROM dbo.master_eosl"
        cursor.execute(query)
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Extract column names
        columns = [column[0] for column in cursor.description]
        
        # Create DataFrame
        table_df = pd.DataFrame.from_records(rows, columns=columns)

        logger.info("Query completed.")

        return table_df
    
    except Exception as e:
        logger.info("Error:", e)
        send_failure_email('run_custom_query', 'Something went wrong while running custom query.', e)
        
    
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
                    FROM dbo.[{table}];
                """
        
        cursor.execute(len_query)
        # max_len = cursor.fetchall()[0][0] if cursor.fetchall()[0][0] > 255 else 255
        max_len = cursor.fetchall()[0][0]

        # Alter  column with change in its length
        alter_query = f"""
        ALTER TABLE dbo.{table}
        ALTER COLUMN [{column}] VARCHAR({max_len});
        """
        cursor.execute(alter_query)

        # create index query
        create_index_query = f"""
                CREATE INDEX IX_{table}_{normalize_column(column)} ON EOSLdatastore.dbo.{table}([{column}]);
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


# alter column length
def alter_col_len(table, column, user, password, db_name, host, port, len=255):
    
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info(f"Database Connection established to alter columne length {table}.{column}")

        # Alter  column with change in its length
        alter_query = f"""
        ALTER TABLE dbo.{table}
        ALTER COLUMN [{column}] VARCHAR({len});
        """
        cursor.execute(alter_query)
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


def create_index_wo_cgl(table, column, user, password, db_name, host, port): # create index without changing the length of column to be indexed
    
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info(f"Database Connection established to create index for {table}.{column}")
    
        # create index query
        create_index_query = f"""
                CREATE INDEX IX_{table}_{normalize_column(column)} ON EOSLdatastore.dbo.{table}([{column}]);
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
    
# Create index with altering the len of column
def create_index_w_len(table, column, user, password, db_name, host, port, length=255): #
    
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info(f"Database Connection established to create index for {table}.{column}")
    
        # Alter  column with change in its length
        alter_query = f"""
        ALTER TABLE dbo.{table}
        ALTER COLUMN [{column}] VARCHAR({length});
        """
        cursor.execute(alter_query)

        # create index query
        create_index_query = f"""
                CREATE INDEX IX_{table}_{normalize_column(column)} ON EOSLdatastore.dbo.{table}([{column}]);
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


def create_managed_eosl_base_table(user, password, db_name, host, port):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        logger.info("Connection established to create the managed_eosl_base table.")

        
        query = """
                
                IF OBJECT_ID('dbo.managed_eosl_base', 'U') IS NOT NULL
                    DROP TABLE dbo.managed_eosl_base;
                
                SELECT 
                    App_Id_Direct AS [Application Ids],
                    App_Name AS [Application Names],
                    CS_Primary_CapabilityCategory AS [Capability List],
                    _id AS [CI Name],
                    App_CLIENT_OWNER AS [Client Owner],
                    CS_Create_Date AS [Create Date], 
                    CS_Disposal_Date AS [Disposal Date],
                    CS_Installation_Date AS [Installation Date],
                    [Assumed HW Expiration Date],
                    App_IT_DIRECTOR AS [IT Director],
                    App_IT_LEAD AS [IT Lead],
                    App_IT_SME AS [IT SME],
                    App_IT_SME_BU AS [IT SME Backup],
                    App_MANAGED_BY AS [Managed By],
                    CS_Modified_Date AS [Modified Date],
                    CS_NERCType AS [NERC Type],
                    CS_OperatingSystem AS [Operating System],
                    CS_OSVendor AS [OS Vendor],
                    CS_OSVersion AS [OS Version],
                    CS_Part_Number AS [Part Number],
                    CS_Domain AS [PGE Domain],
                    CS_Primary_Capability AS [Primary Capability],
                    CS_Primary_CapabilityName AS [Primary Capability Name],
                    CS_Item AS [Product Category - Tier 3],
                    CS_Model_Number AS [Product Name],
                    CS_Site AS [Site+],
                    CS_AssetLifeCycleStatusName AS [Status],
                    CS_System_Environment AS [System Environment],
                    CS_Tag_Number AS [Tag Number],
                    App_BIA_TIER as [BIA Tier],
                    [ss_ids],
                    [db_ids]
                INTO dbo.managed_eosl_base
                FROM EOSLdatastore.dbo.view_itassets_managed_services
                """
        
        
        cursor.execute(query)        

        alter_query = f"""
        ALTER TABLE dbo.managed_eosl_base
        ALTER COLUMN [CI Name] VARCHAR(255);
        """
        cursor.execute(alter_query)

        # create Correct CI name column(that remove the domain from name) and add index to it, as we will use it for lookup
        correct_col_query = """
                ALTER TABLE EOSLdatastore.dbo.managed_eosl_base
                ADD [CIName_Correct] AS (
                    CASE 
                        WHEN CHARINDEX('.', [CI Name]) > 0 
                            THEN LEFT([CI Name], CHARINDEX('.', [CI Name]) - 1)
                        ELSE [CI Name]
                    END
                ) PERSISTED;

                CREATE INDEX IX_managed_eosl_base_CIName_Correct 
                    ON EOSLdatastore.dbo.managed_eosl_base([CIName_Correct]);

        """
        cursor.execute(correct_col_query)
        
        conn.commit()
        
    except Exception as e:
        print("Error:", e)
        send_failure_email('create_managed_eosl_base_table', 'Something went wrong while loading base managed assets table data into the database.', e)
        

    finally:
        try:
            cursor.close()
            conn.close()
            print(" Connection closed.")
        except:
            pass



def create_base_master_table(user, password, db_name, host, port):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("Connection established to create the master_eosl_base table.")

        
        query = """
                
                IF OBJECT_ID('dbo.master_eosl_base', 'U') IS NOT NULL
                    DROP TABLE dbo.master_eosl_base;
                
                SELECT 
                    App_Id_Direct AS [Application Ids],
                    App_Name AS [Application Names],
                    CS_Primary_CapabilityCategory AS [Capability List],
                    CS_Name AS [CI Name],
                    App_CLIENT_OWNER AS [Client Owner],
                    CS_Create_Date AS [Create Date],
                    CS_Disposal_Date AS [Disposal Date],
                    CS_Installation_Date AS [Installation Date],
                    [Assumed HW Expiration Date],
                    App_IT_DIRECTOR AS [IT Director],
                    App_IT_LEAD AS [IT Lead],
                    App_IT_SME AS [IT SME],
                    App_IT_SME_BU AS [IT SME Backup],
                    App_MANAGED_BY AS [Managed By],
                    CS_Modified_Date AS [Modified Date],
                    CS_NERCType AS [NERC Type],
                    CS_OperatingSystem AS [Operating System],
                    CS_OSVendor AS [OS Vendor],
                    CS_OSVersion AS [OS Version],
                    CS_Part_Number AS [Part Number],
                    CS_Serial_Number As [Serial Number],
                    CS_Domain AS [PGE Domain],
                    CS_Primary_Capability AS [Primary Capability],
                    CS_Primary_CapabilityName AS [Primary Capability Name],
                    CS_Item AS [Product Category - Tier 3],
                    CS_Model_Number AS [Product Name],
                    CS_Site AS [Site+],
                    CS_AssetLifeCycleStatusName AS [Status],
                    CS_System_Environment AS [System Environment],
                    CS_Tag_Number AS [Tag Number],
                    App_BIA_TIER as [BIA Tier]
                INTO dbo.master_eosl_base
                FROM dbo.view_itassets
                WHERE  [CS_AssetLifeCycleStatusName] in ('Deployed', 'Missing', 'Down');
                
                """
        
        
        cursor.execute(query)

        create_col_query = """
                        ALTER TABLE dbo.master_eosl_base
                        ADD ss_ids NVARCHAR(100),
                            db_ids NVARCHAR(100);
                    """
        cursor.execute(create_col_query)


        ########### currently not using this
        # # get the max lenght of column CI Name
        # len_query = """
        #     SELECT MAX(LEN([CI Name])) AS MaxLength
        #         FROM dbo.master_eosl;
        #         """
        
        # cursor.execute(len_query)
        # max_len_ci_name = cursor.fetchall()[0][0] + 10
        # print('max_length:',max_len_ci_name)

        # alter_query = f"""
        # ALTER TABLE dbo.master_eosl_base
        # ALTER COLUMN [CI Name] VARCHAR(255);
        # """
        ###########
        

        alter_query = f"""
        ALTER TABLE dbo.master_eosl_base
        ALTER COLUMN [CI Name] VARCHAR(255);
        """
        cursor.execute(alter_query)

        # create Correct CI name column(that remove the domain from name) and add index to it, as we will use it for lookup
        correct_col_query = """
                ALTER TABLE EOSLdatastore.dbo.master_eosl_base
                ADD [CIName_Correct] AS (
                    CASE 
                        WHEN CHARINDEX('.', [CI Name]) > 0 
                            THEN LEFT([CI Name], CHARINDEX('.', [CI Name]) - 1)
                        ELSE [CI Name]
                    END
                ) PERSISTED;
                CREATE INDEX IX_master_eosl_base_CIName_Correct ON EOSLdatastore.dbo.master_eosl_base([CIName_Correct]);

        """
        cursor.execute(correct_col_query)
        
        conn.commit()
        
    except Exception as e:
        print("Error:", e)
        send_failure_email('create_base_master_table', 'Something went wrong while loading base master table data into the database.', e)
        

    finally:
        try:
            cursor.close()
            conn.close()
            print(" Connection closed.")
        except:
            pass

def merge_base_master_n_managed(user, password, db_name, host, port):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        logger.info("Connection established to merge base master and base managed table.")

        
        merge_query = """
                INSERT INTO dbo.master_eosl_base (
                [Application Ids],
                [Application Names],
                [Capability List],
                [CI Name],
                [Client Owner],
                [Create Date],
                [Disposal Date],
                [Installation Date],
                [Assumed HW Expiration Date],
                [IT Director],
                [IT Lead],
                [IT SME],
                [IT SME Backup],
                [Managed By],
                [Modified Date],
                [NERC Type],
                [Operating System],
                [OS Vendor],
                [OS Version],
                [Part Number],
                [PGE Domain],
                [Primary Capability],
                [Primary Capability Name],
                [Product Category - Tier 3],
                [Product Name],
                [Site+],
                [Status],
                [System Environment],
                [Tag Number],
                [BIA Tier],
                [ss_ids],
                [db_ids]
            )
            SELECT 
                [Application Ids],
                [Application Names],
                [Capability List],
                [CI Name],
                [Client Owner],
                [Create Date],
                [Disposal Date],
                [Installation Date],
                [Assumed HW Expiration Date],
                [IT Director],
                [IT Lead],
                [IT SME],
                [IT SME Backup],
                [Managed By],
                [Modified Date],
                [NERC Type],
                [Operating System],
                [OS Vendor],
                [OS Version],
                [Part Number],
                [PGE Domain],
                [Primary Capability],
                [Primary Capability Name],
                [Product Category - Tier 3],
                [Product Name],
                [Site+],
                [Status],
                [System Environment],
                [Tag Number],
                [BIA Tier],
                [ss_ids],
                [db_ids]
            FROM dbo.managed_eosl_base;
                """

        
        cursor.execute(merge_query)

        conn.commit()
    except Exception as e:
        print("Error:", e)
        send_failure_email('create_merge_base_master_n_managed', 'Something went wrong while merging base master and base managed table.', e)
        
    finally:
        try:
            cursor.close()
            conn.close()
            print(" Connection closed.")
        except:
            pass

def create_filtered_nas_report_table(user, password, db_name, host, port):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("Connection established to create nas_report_filter table.")

        
        create_query = """
                DROP TABLE IF EXISTS dbo.nas_report_filter;
                
                CREATE TABLE dbo.nas_report_filter (
                    [APP-ID] nvarchar(max) NULL,
                    [Frame Name] nvarchar(max) NULL,
                    [Cluster] nvarchar(255) NULL,
                    [Allocated] float NULL,
                    [Used] float NULL
                );
                """

        
        cursor.execute(create_query)

        insert_query = """
        INSERT INTO dbo.nas_report_filter
        SELECT DISTINCT
                    n.[APP-ID],
                    n.[Frame Name],
                    n.[Cluster],
                    n.[Allocated],
                    n.[Used]
                
        FROM EOSLdatastore.dbo.master_eosl_base m
        LEFT JOIN EOSLdatastore.dbo.nas_report n
            ON n.[APP-ID] = m.[Application Ids]
        WHERE m.[Capability List] = 'Server' and n.[APP-ID] is not null;
        """

        cursor.execute(insert_query)
        conn.commit()
    except Exception as e:
        print("Error:", e)
        send_failure_email('create_filtered_nas_report_table', 'Something went wrong while loading filtered nas report table into the database.', e)
        
    finally:
        try:
            cursor.close()
            conn.close()
            print(" Connection closed.")
        except:
            pass


def create_filtered_view_database_table(user, password, db_name, host, port):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("Connection established to create view_database_assets_filter table.")

        
        create_query = """
                DROP TABLE IF EXISTS dbo.view_database_assets_filter;
                
                CREATE TABLE dbo.view_database_assets_filter (
                    [DB_HostName] nvarchar(max) NULL,
                    [DB_Model] nvarchar(max) NULL,
                    [DB_Short_Description] nvarchar(max) NULL,
                    [DB_version_number] nvarchar(255) NULL,
                    [DB_Version_Short] nvarchar(255) NULL
                );
                """

        
        cursor.execute(create_query)

        insert_query = """
        INSERT INTO dbo.view_database_assets_filter
        SELECT DISTINCT
                d.[DB_HostName],
                d.[DB_Model],
                d.[DB_Short_Description],
                d.[DB_version_number],
                d.[DB_Version_Short]
                
        FROM EOSLdatastore.dbo.master_eosl_base m
        LEFT JOIN (
            SELECT 
                DB_HostName,
                MAX(DB_Model) AS DB_Model,
                MAX(DB_version_number) AS DB_version_number,
                MAX(DB_Version_Short) AS DB_Version_Short,
                STRING_AGG(CAST(DB_Short_Description AS NVARCHAR(MAX)), ', ') AS DB_Short_Description
                -- STRING_AGG(DB_Short_Description, ', ') AS DB_Short_Description (commented)
            FROM EOSLdatastore.dbo.view_database_assets
            GROUP BY DB_HostName
        ) d
        ON m.[CIName_Correct] = d.[DB_HostName]
        WHERE d.[DB_HostName] is not null;
        """

        cursor.execute(insert_query)
        conn.commit()

        

    except Exception as e:
        print("Error:", e)
        send_failure_email('create_filtered_view_database_table', 'Something went wrong while loading filtered_view_database_assets table into the database.', e)

    finally:
        try:
            cursor.close()
            conn.close()
            print(" Connection closed.")
        except:
            pass


def create_filtered_view_database_managed_services_table(user, password, db_name, host, port):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("Connection established to create view_database_managed_services_filter table.")

        
        create_query = """
                DROP TABLE IF EXISTS dbo.view_database_managed_services_filter;
                
                CREATE TABLE dbo.view_database_managed_services_filter (
                    [DB_Instance_Id] nvarchar(max) NULL,
                    [DB_Model] nvarchar(max) NULL,
                    [DB_Short_Description] nvarchar(max) NULL,
                    [DB_version_number] nvarchar(255) NULL,
                    [DB_Version_Short] nvarchar(255) NULL
                );
                """

        
        cursor.execute(create_query)

        insert_query = """
        INSERT INTO dbo.view_database_managed_services_filter (
            [DB_Instance_Id],
            [DB_Model],
            [DB_Short_Description],
            [DB_version_number],
            [DB_Version_Short]
        )
        SELECT 
            d.[DB_Instance_Id],
            d.[DB_Model],
            d.[DB_Short_Description],
            d.[DB_version_number],
            d.[DB_Version_Short]
        FROM EOSLdatastore.dbo.master_eosl_base m
        JOIN dbo.view_database_managed_services d
            ON m.[db_ids] = d.[DB_Instance_Id];
        """

        cursor.execute(insert_query)
        conn.commit()

        

    except Exception as e:
        print("Error:", e)
        send_failure_email('create_filtered_view_database_table', 'Something went wrong while loading filtered_view_database_assets table into the database.', e)

    finally:
        try:
            cursor.close()
            conn.close()
            print(" Connection closed.")
        except:
            pass


def create_master_eosl_table(user, password, db_name, host, port):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("Connection established to create maseter_eosl table.")

        
        # -- We will perform joins on the base master table with other tables using different columns.
        # -- To optimize these joins, we first create indexes on the columns that will be used for lookups.
        # -- Before creating indexes, modify column types since many are VARCHAR(MAX),
        # -- which cannot be indexed efficiently.
        # -- We plan to join VMware table on [App ID] with [Application Ids] in the base table,
        # -- so we first adjust the [Application Ids] column type to support indexing.
        

        create_query = """
                
    DROP TABLE IF EXISTS dbo.master_eosl;

        CREATE TABLE [dbo].[master_eosl](
        [Application Ids] [nvarchar](max) NULL,
        [Application Names] [nvarchar](max) NULL,
        [Capability List] [nvarchar](max) NULL,
        [CI Name] [nvarchar](max) NULL,
        [Client Owner] [nvarchar](max) NULL,
        [Create Date] [date] NULL,
        [Disposal Date] [date] NULL,
        [Installation Date] [date] NULL,
        [Assumed HW Expiration Date] [date] NULL,
        [IT Director] [nvarchar](max) NULL,
        [IT Lead] [nvarchar](max) NULL,
        [IT SME] [nvarchar](max) NULL,
        [IT SME Backup] [nvarchar](max) NULL,
        [Managed By] [nvarchar](max) NULL,
        [Modified Date] [date] NULL,
        [NERC Type] [float] NULL,
        [Operating System] [nvarchar](max) NULL,
        [OS Vendor] [nvarchar](max) NULL,
        [OS Version] [nvarchar](max) NULL,
        [Part Number] [nvarchar](max) NULL,
        [Serial Number] [nvarchar](max) NULL,
        [PGE Domain] [nvarchar](max) NULL,
        [Primary Capability] [nvarchar](max) NULL,
        [Primary Capability Name] [nvarchar](max) NULL,
        [Product Category - Tier 3] [nvarchar](max) NULL,
        [Product Name] [nvarchar](max) NULL,
        [Site+] [nvarchar](max) NULL,
        [Status] [nvarchar](max) NULL,
        [System Environment] [nvarchar](max) NULL,
        [Tag Number] [nvarchar](max) NULL,
        [BIA Tier] [nvarchar](max) NULL,
        [CIName_Correct] [nvarchar](max) NULL,
        [CPU] [float] NULL,
        [RAM GB] [float] NULL,
        [Storage Allocated TB] [float] NULL,
        [Storage Used TB] [float] NULL,
        [Storage Used Source] [nvarchar](50) NULL,   -- changed to NULL to avoid insert failures
        [Cluster] [nvarchar](max) NULL,
        [Host] [nvarchar](max) NULL,
        [Vcenter] [nvarchar](max) NULL,
        [SAN Storage Frames] [nvarchar](max) NULL,
        [NAS Storage Frames] [nvarchar](max) NULL,
        [SAN Model Expiry] [date] NULL,
        [NAS Frame Expiry] [date] NULL,
        [SAN Model Name] [nvarchar](max) NULL,
        [NAS Model Name] [nvarchar](max) NULL,
        [NAS Allocated Storage space in TB] [float] NULL,
        [NAS Used Storage space in TB] [float] NULL,
        [DDBoost backup Datadomain] [nvarchar](255) NULL,
        [DB Type] [nvarchar](max) NULL,
        [Database] [nvarchar](max) NULL,
        [DB version] [nvarchar](max) NULL,
        [Avamar Backup Datadomain] [nvarchar](max) NULL,
        [MW Instance Name] [nvarchar](max) NULL,
        [MW Version] [nvarchar](max) NULL,
        [OS Expiry Date] [date] NULL,
        [DB EOSL] [date] NULL,
        [MW EOSL] [date] NULL
    );

                """

        cursor.execute(create_query)

        insert_query = """
        INSERT INTO dbo.master_eosl
    SELECT DISTINCT
        m.[Application Ids],
        m.[Application Names],
        m.[Capability List],
        m.[CI Name],
        m.[Client Owner],
        m.[Create Date],
        m.[Disposal Date],
        m.[Installation Date],
        m.[Assumed HW Expiration Date],
        m.[IT Director],
        m.[IT Lead],
        m.[IT SME],
        m.[IT SME Backup],
        m.[Managed By],
        m.[Modified Date],
        m.[NERC Type],
        m.[Operating System],
        m.[OS Vendor],
        m.[OS Version],
        m.[Part Number],
        m.[Serial Number],
        m.[PGE Domain],
        m.[Primary Capability],
        m.[Primary Capability Name],
        m.[Product Category - Tier 3],
        m.[Product Name],
        m.[Site+],
        m.[Status],
        m.[System Environment],
        m.[Tag Number],
        m.[BIA Tier],
        m.[CIName_Correct],

        COALESCE(v.[Virtual CPU], e.[vCPU Allocated]) AS [CPU],
        COALESCE(v.[Memory], e.[Host Memory | GB]) AS [RAM GB],
        COALESCE(v.[Total Disk Space], e.[Datastore Disk Space], s.[Total Size (TB)]) AS [Storage Allocated TB],
        COALESCE(v.[Disk Utlization (TB)], e.[Disk Utilization], s.[Used (TB)]) AS [Storage Used TB],

        CASE 
            WHEN v.[Disk Utlization (TB)] IS NOT NULL THEN 'VMware'
            WHEN e.[Disk Utilization] IS NOT NULL THEN 'ESXi'
            WHEN s.[Used (TB)] IS NOT NULL THEN 'SAN'
            ELSE 'Unknown'
        END AS [Storage Used Source],

        v.[Cluster],
        v.[Current Host],
        v.[vCenter],
        s.[SystemDisplayName] AS [SAN Storage Frames],
        n.[Frame Name] AS [NAS Storage Frames],
        sa1.[Dell's Planned year to remediate] AS [SAN Model Expiry],
        sa2.[Dell's Planned year to remediate] AS [NAS Frame Expiry],
        sa1.[Model] AS [SAN Model Name],
        sa2.[Model] AS [NAS Model Name],
        n.[Allocated] AS [NAS Allocated Storage space in TB],
        n.[Used] AS [NAS Used Storage space in TB],
        dd.[System] AS [DDBoost backup Datadomain],
        COALESCE(d.[DB_Model], dm.[DB_Model]) AS [DB Type],
        COALESCE(d.[DB_Short_Description], dm.[DB_Short_Description]) AS [Database],
        COALESCE(d.[DB_version_number], dm.[DB_version_number]) AS [DB version],
        COALESCE(a.[Media Server], p.[Media Server]) AS [Avamar Backup Datadomain],
        COALESCE(mw.[SS_Model], mwm.[SS_Model]) AS [MW Instance Name],
        COALESCE(mw.[SS_Version_Number], mwm.[SS_Version_Number]) AS [MW Version],
        ea.[End Date] AS [OS Expiry Date],
        COALESCE(ea2.[End Date], ea2b.[End Date]) AS [DB EOSL],
        COALESCE(ea3.[End Date], ea3b.[End Date]) AS [MW EOSL]
        
        

    FROM EOSLdatastore.dbo.master_eosl_base m
    LEFT JOIN EOSLdatastore.dbo.VMware v 
        ON v.[VM Name] = m.[CIName_Correct]
    LEFT JOIN EOSLdatastore.dbo.ESXi e 
        ON m.[CIName_Correct] = e.[SD_Name]
    LEFT JOIN EOSLdatastore.dbo.san_report s
        ON m.[CIName_Correct] = s.[ServerName]
    LEFT JOIN EOSLdatastore.dbo.nas_report_filter n
        ON m.[Application Ids] = n.[APP-ID]
    LEFT JOIN EOSLdatastore.dbo.ddboost_report dd
        ON m.[CIName_Correct] = dd.[ClientName]
    LEFT JOIN EOSLdatastore.dbo.storage_analysis sa1
        ON s.[SystemDisplayName] = sa1.[System Name]
    LEFT JOIN EOSLdatastore.dbo.storage_analysis sa2
        ON n.[Cluster] = sa2.[System Name]
    LEFT JOIN EOSLdatastore.dbo.view_database_assets_filter d
        ON m.[CIName_Correct] = d.[DB_HostName]
    LEFT JOIN EOSLdatastore.dbo.view_database_managed_services dm
        ON m.[db_ids] = dm.[DB_Instance_Id]
    LEFT JOIN EOSLdatastore.dbo.avamar_servers a
        ON m.[CIName_Correct] = a.[Client]
    LEFT JOIN EOSLdatastore.dbo.ppdm_servers p
        ON m.[CIName_Correct] = p.[Client]
    LEFT JOIN EOSLdatastore.dbo.view_middleware_assets mw 
        ON m.[CIName_Correct] = mw.[SS_Name]
    LEFT JOIN EOSLdatastore.dbo.view_middleware_managed_services mwm 
        ON m.[ss_ids] = mwm.[SS_Instance_Id]
    LEFT JOIN EOSLdatastore.dbo.EOSL_assets ea
        ON m.[Operating System] LIKE '%' + ea.[Corrected Name] + '%'   
    LEFT JOIN EOSLdatastore.dbo.EOSL_assets ea2
        ON ea2.[Short_Version] = d.[DB_Version_Short]
        AND ea2.[Model] = d.[DB_Model]
    LEFT JOIN EOSLdatastore.dbo.EOSL_assets ea2b
        ON ea2b.[Short_Version] = dm.[DB_Version_Short]
        AND ea2b.[Model] = dm.[DB_Model]
    LEFT JOIN EOSLdatastore.dbo.EOSL_assets ea3
        ON mw.[SS_Version_Number] LIKE ea3.[Version] + '%'
        AND ea3.[Type] IN ('MW Web Server', 'MW APP Server')
    LEFT JOIN EOSLdatastore.dbo.EOSL_assets ea3b
        ON mwm.[SS_Version_Number] LIKE ea3b.[Version] + '%'
        AND ea3b.[Type] IN ('MW Web Server', 'MW APP Server');
        """
        cursor.execute(insert_query)
        
        conn.commit()

    except Exception as e:
        print("Error:", e)
        send_failure_email('create_master_eosl_table', 'Something went wrong while loading master eosl table into the database.', e)

    finally:
        try:
            cursor.close()
            conn.close()
            print(" Connection closed.")
        except:
            pass




def load_nas_report_into_vital(eosldb_username,eosldb_password, eosldb_name, eosldb_host, eosldb_port, vitaldb_username, vitaldb_password, vitaldb_name, vitaldb_host, vitaldb_port, table_name='nas_report', environment='vital production'):
    try:
        logger.info(f"Starting to load nas report data into vital database in {environment} environment.")
        # fetch the nas report data from nas_report table in EOSL datastore database
        select_query = f"""
                SELECT * from dbo.nas_report;
                """
        df_nas_report = fetch_table_data(select_query, eosldb_username,eosldb_password, eosldb_name, eosldb_host, eosldb_port)
        
        # load the nas report data into the nas_report table in vital database
        load_data_into_db(df_nas_report, table_name, vitaldb_username, vitaldb_password, vitaldb_name, vitaldb_host, vitaldb_port)
        

    except Exception as e:
        print("Error:", e)
        send_failure_email('load_nas_report_into_vital', f'Something went wrong while loading nas report data into the vital database for {environment}.', e)

    


# Function to fetch table data from datbase 
def load_ddboost_client(user, password, db_name, host, port):
    
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={db_name};UID={user};PWD={password}"
        )
        cursor = conn.cursor()
        logger.info("Database Connection established to fetch table data.")
    
        # Run SELECT query
        query = "Select DISTINCT [Client] from dbo.ddboost_report;"
        cursor.execute(query)
        
        logger.info("Query completed.")

        # Fetch all rows
        rows = cursor.fetchall()
        
        # Extract column names
        columns = [column[0] for column in cursor.description]
        
        # Create DataFrame
        table_df = pd.DataFrame.from_records(rows, columns=columns)

        # Add created_at column with current timestamp
        table_df.loc[:, 'created_at'] = pd.Timestamp.utcnow()

        # load this data into the table
        load_data_into_db(table_df, 'ddboost_clients', user, password, db_name, host, port)

    
    except Exception as e:
        logger.info("Error:", e)
        send_failure_email('run_custom_query', 'Something went wrong while running custom query.', e)
        
    
    finally:
        try:
            cursor.close()
            conn.close()
            logger.info(" Connection closed.")
        except:
            pass


import numpy as np
import pandas as pd
import json
import logging
from src.utils import convert_into_tb, extract_version_tuple, send_failure_email, mod_list_col
from src.load import fetch_table_data, load_data_into_db, run_custom_query, create_index, create_index_wo_cgl, alter_col_len

# setup loggers
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# transform VSphere data
def transform_vsphere_string(st):
    try:
        st = json.loads(st)
        return [f"<{i['category']}-{i['name']}>" for i in st]
    except:
        # send_failure_email('transform_vsphere_string', 'Something went wrong while transforming vsphere string')
        return "none"

# Convert each VM's property list into dictionary
def flatten_vrops_data(metric_names, results, resourceKind='VirtualMachine'):
    try:
        # Flatten the data
        flattened_data = []
        # for vm_props in res:
        logger.info('Start flattening the data for {resourceKind}')
        for vm_props in results:
            # if item['data'][0]['name'] in metric_names:
            vm_dict = {item['name']: item['value'] for item in vm_props['data'] if item['name'] in metric_names} 
            flattened_data.append(vm_dict)
        
        # return flatten data
        return flattened_data
    except:
        send_failure_email('flatten_vrops_data', 'Something went wrong while flattening vrops data')

# Transform Virtual Machine Data
def transform_vmware_data(flatten_vmware_data, vmware_column_mapping):
    try:
        logger.info('Start Transforming VirtualMachine data')
        # convert the data into dataframe
        df_vmware = pd.DataFrame(flatten_vmware_data)

        # rename columns
        df_vmware.rename(columns=vmware_column_mapping, inplace=True)
        
        # preserve order
        df_vmware = df_vmware[vmware_column_mapping.values()]

        # clean the VM Name column by removing(-_)
        df_vmware['VM Name'] = df_vmware['VM Name'].str.split(r'[_-]').str[0]

        # apply transformation on vSphere
        df_vmware['Vsphere Tags'] = df_vmware['Vsphere'].apply(transform_vsphere_string)
        df_vmware.drop(columns = ['Vsphere'], inplace=True)

        # Memory in KB, converting in MB
        df_vmware["Memory"] = df_vmware['Memory'].astype(float) / (1024**2)

        # Convert Memory Utilization from GB to TB
        df_vmware['Memory Utilization'] = round(df_vmware['Memory Utilization'].astype(float) / (1024**2), 2)

        # total disk in GB converting into TB
        df_vmware['Total Disk Space'] = round(df_vmware['Total Disk Space'].astype(float) / 1024, 2)

        # Disk Space/Utilization in GB converting into TB
        df_vmware['Disk Utlization (TB)'] = round(df_vmware['Disk Utlization (TB)'].astype(float) / 1024, 2)

        # Calculate disk capacity remaining and convert into TB(from GB)
        df_vmware['Disk Capacity'] = df_vmware['Disk Capacity'] - df_vmware['Disk Utlization (TB)'] 
        df_vmware['Disk Capacity'] = round(df_vmware['Disk Capacity'] / 1024, 2) # convvert into tb
        df_vmware.rename(columns={"Disk Capacity": "Disk Capacity Remaining"}, inplace=True)

        # round off CPU usage
        df_vmware['CPU Usage'] =  round(df_vmware['CPU Usage'], 4)
        
        # Save as Excel file
        # Save the data as excel file
        df_vmware.to_excel('data/processed/new_fetched_vm.xlsx', index=False)
        logger.info("VirtualMachine Data Saved as Excel File")
        
        # Change type for Vsphere type (For DB loading only, as list type not supported in sql server)
        df_vmware['Vsphere Tags'] =  df_vmware['Vsphere Tags'].astype(str)
        logger.info("Transforming VirtualMachine Data Completed.")

        # return dataframe
        return df_vmware
    except Exception as e:
        logger.info(f'Something went wrong while transforming vmware data:{e}')
        send_failure_email('transform_vmware_data', 'Something went wrong while transforming vmware data')


# Transform ESXi Host Data
def transform_esxi_data(flatten_esxi_data, esxi_column_mapping):
    try:
        logger.info('Start Transforming VirtualMachine data')
        # convert the data into dataframe
        df_esxi = pd.DataFrame(flatten_esxi_data)

        # rename columns
        df_esxi.rename(columns=esxi_column_mapping, inplace=True)
        
        # preserve order
        df_esxi = df_esxi[esxi_column_mapping.values()]

        # transform Mgm IP column by having the last item from the ip list
        df_esxi['Mgm IP'] = df_esxi['Mgm IP'].str.split(',').str[-1]
        
        # Transform Datastore Disk Space unit from bytes to TB 
        df_esxi['Datastore Disk Space'] = (df_esxi['Datastore Disk Space'].astype(float) / (1024**4)).round(2)
        
        # make Host CPU usage % round by 2
        df_esxi['Host CPU Usage %'] = df_esxi['Host CPU Usage %'].round(2)

        # TransforHost Memory | GBm Host Memory Allocated(MB) into GB
        df_esxi['Host Memory | GB'] = (df_esxi['Host Memory | GB'].astype(float) / (1024 ** 2)).round(2) 

        # round off the Memory Reserve perc
        df_esxi['Host Mem Usage %'] = df_esxi['Host Mem Usage %'].round(2)

        # round off the Memory Reserve perc
        df_esxi['Memory Reserved %'] = df_esxi['Memory Reserved %'].round(2)

        # Convert System Uptime (sec) into Day(s)
        df_esxi['System|Uptime (Day(s))'] = (df_esxi['System|Uptime (Day(s))'] / (60*60*24)).round(2)

        # get and store the sub domain name from the domain name in new column
        df_esxi['SD_Name'] = df_esxi['Name'].str.split('.').str[0]

        # Convert Disk Utilization  (GB) into (TB)
        df_esxi['Disk Utilization'] = (df_esxi['Disk Utilization | GB'] / (1024)).round(2)
        df_esxi.drop(columns=['Disk Utilization | GB'], inplace=True)


        # Save as Excel file
        # Save the data as excel file
        df_esxi.to_excel('data/processed/new_fetched_esxi.xlsx', index=False)
        logger.info("ESXi Host Data Saved as Excel File")
        
        logger.info("Transforming ESXi Host Data Completed.")

        # return dataframe
        return df_esxi
    except Exception as e:
        logger.info(f'Something went wrong while transforming esxi data:{e}')
        send_failure_email('transform_esxi_data', 'Something went wrong while transforming esxi data')


# Transform NAS data
def transform_nas_data(dataframes, master_df):
    try:
        logger.info('Start Transforming NAS data')
        
        # concatenate all the dataframes
        nas_df = pd.concat(dataframes)

        # Convert the Units in TB, Apply the function row-wise
        nas_df['Allocated'] = nas_df.apply(lambda row: convert_into_tb(row['Allocated Size'], row['Allocated Unit']), axis=1)
        nas_df['Used'] = nas_df.apply(lambda row: convert_into_tb(row['Used Size'], row['Used Unit']), axis=1)

        # rename columns for nas df
        nas_df.rename(columns={'APP-IDs from Share Descriptions': 'APP-ID'}, inplace=True)

        # select only required columns from the nas and master dataframe
        nas_df = nas_df[['Path', 'Allocated', 'Used', 'APP-ID', 'Clients', 'Cluster']]
        master_df = master_df[['Path', 'APP-ID', 'Frame Name']]

        # merge both dfs
        # merging on 'Path' and also we have APP-ID in common, which we will merge below
        new_df = pd.merge(nas_df, master_df, on=['Path'], how='left')

        # Create a unified APP-ID column, as after merging we have two APP-ID
        new_df['APP-ID'] = new_df['APP-ID_x'].combine_first(new_df['APP-ID_y'])

        # Drop the old columns
        new_df.drop(columns=['APP-ID_x', 'APP-ID_y'], inplace=True)

        # Split the APP-ID column by space: Step 1, as some rows having multiple APP-Id in single cell
        new_df['APP-ID'] = new_df['APP-ID'].str.split()
        # Explode the list into separate rows: Step 2
        new_df = new_df.explode('APP-ID').reset_index(drop=True)

        # Step 1: Split the Clients column by space
        new_df['Clients'] = new_df['Clients'].str.split()
        # Step 2: Explode the list into separate rows
        new_df = new_df.explode('Clients').reset_index(drop=True)

        # Save as Excel file
        # Save the data as excel file
        new_df.to_excel('data/processed/merged_nas_report.xlsx', index=False)
        logger.info("NAS Data Saved as Excel File")
        
        logger.info("Transforming NAS Data Completed.")

        # return dataframe
        return new_df
    except Exception as e:
        logger.info(f'Something went wrong while transforming nas data:{e}')
        send_failure_email('transform_nas_data', 'Something went wrong while transforming nas data')


# Transform AIOPS data
def transform_aiops_data(aiops_df, master_df):
    try:
        logger.info('Transforming AIOPS(SAN) data Initialized...')
        # transform data
        aiops_df['Total Size (TB)'] = (aiops_df['total_size']/(1024**4)).round(2)
        aiops_df['Used (TB)'] = (aiops_df['allocated_size']/(1024**4)).round(2)
        aiops_df.drop(columns=['id','allocated_size','total_size'], inplace=True)
        # rename columns
        aiops_df.rename(columns = {"name":"StorageGroupName"}, inplace= True)

        # modify master_df
        master_df.drop(columns=['TotalSize(TB)', 'Used(TB)'],inplace=True)
        merged_aiops_df = pd.merge(master_df, aiops_df,on='StorageGroupName', how='left')
        # save the excel file as well
        merged_aiops_df.to_excel('data/processed/merged_aiops.xlsx', index=False)

        logger.info("Transforming AIOPS(SAN) Data Completed.")

        # return
        return merged_aiops_df
    except Exception as e:
        logger.info(f'Something went wrong while transforming aiops data:{e}')
        send_failure_email('transform_aiops_data', 'Something went wrong while transforming aiops data')


# Transform IBM data
def transform_ibm_data(ibm_df, master_df):
    try:
        logger.info('Transforming IBM(SAN) data Initialized...')
        # select only required columns
        ibm_df = ibm_df[['name', 'san_capacity_bytes', 'used_san_capacity_bytes']]
        
        # convert capacity into TB and store it in new columns  
        ibm_df['Total Size (TB)'] = round(ibm_df['san_capacity_bytes'] / (1024**4), 2)
        ibm_df['Used (TB)'] = round(ibm_df['used_san_capacity_bytes'] / (1024**4), 2)
        # drop old bytes columns
        ibm_df.drop(columns=['san_capacity_bytes','used_san_capacity_bytes'], inplace=True)

        # rename columns
        ibm_df.rename(columns={'name':'ServerName'}, inplace=True)
        
        # merge the ibm_df with the master_df to do vlookup on servername
        merged_ibm = pd.merge(master_df, ibm_df, on='ServerName', how='left')
        
        # store transform file as excel
        merged_ibm.to_excel('data/processed/merged_ibm.xlsx', index=False)
        
        logger.info("Transforming IBM(SAN) Data Completed.")

        # return 
        return merged_ibm
    except Exception as e:
        logger.info(f'Something went wrong while transforming ibm data:{e}')
        send_failure_email('transform_ibm_data', 'Something went wrong while transforming ibm data')


# Transform AIOPS data
def transform_aiops_storage_data(aiops_df):
    try:
        logger.info('Transforming AIOPS(Storage) data Initialized...')
        # transform data
        # Aplly filter on system type
        aiops_df = aiops_df[~aiops_df['system_type'].isin(['POWERSTORE','POWERVAULT'])]

        # Select only required columns
        aiops_df = aiops_df[['name', 'model', 'serial_number', 'configured_size', 'used_size', 'free_size', 'contract_expiration_date_timestamp']]

        # Convert all the size column into PB
        # define 1PB 
        PB = 1024**5
        aiops_df = aiops_df.assign(
            configured_size = aiops_df['configured_size'] / PB,
            used_size = aiops_df['used_size'] / PB,
            free_size = aiops_df['free_size'] / PB
        )# dividing every columns data with PB

        # rename the columns
        aiops_df = aiops_df.rename(columns={'name':'System Name', 'model':'Model', 'serial_number':'Serial Number',
                                        'configured_size':'Capacity', 'used_size':'Allocated', 'free_size':'Free',
                                        'contract_expiration_date_timestamp':'Dell\'s Planned year to remediate'})
        

        logger.info("Transforming AIOPS(Storage) Data Completed.")

        # return
        return aiops_df
    except Exception as e:
        logger.info(f'Something went wrong while transforming aiops storage data:{e}')
        send_failure_email('transform_aiops_storage_data', 'Something went wrong while transforming aiops storage data')


# Transform IBM data
def transform_ibm_storage_data(ibm_df):
    try:
        logger.info('Transforming IBM(Storage) data Initialized...')
        # Convert all the size column into GiB
        # define 1GiB 
        GIB = 1024**3
        PB = 1024**5
        ibm_df = ibm_df.assign(
            physical_capacity = ibm_df['physical_capacity'] / PB,
            used_capacity_bytes = ibm_df['used_capacity_bytes'] / PB,
            available_capacity_bytes = ibm_df['available_capacity_bytes'] / PB
        )# dividing every columns data with PB

        # Select only required columns
        ibm_df = ibm_df[['name', 'model', 'enclosure_node_serial_number', 'physical_capacity', 'used_capacity_bytes', 'available_capacity_bytes']]
        
        # rename the columns
        ibm_df = ibm_df.rename(columns={'name':'Name', 'model':'Model', 'enclosure_node_serial_number':'Serial Number',
                                'physical_capacity':'Usable Capacity', 'used_capacity_bytes':'Used Capacity',
                                'available_capacity_bytes':'Availabe Capacity','contract_expiration_date_timestamp':'Dell\'s Planned year to remediate'})


        logger.info("Transforming IBM(Storage) Data Completed.")

        # return 
        return ibm_df
    except Exception as e:
        logger.info(f'Something went wrong while transforming ibm(Storage) data:{e}')
        send_failure_email('transform_ibm_storage_data', 'Something went wrong while transforming ibm(Storage) data')

# Transform AMPs Data
def transform_amps_data(df_view, view_type=None):
    try:
        # for view type = view_middleware_assets
        if view_type == 'view_middleware_assets':
            # get the server name from the 'SS_Name' 
            # because some SS_names are i.e= WildFly 12.0 identified as JBossPhysicalInventory on tsitinfapplx007.comp.pge.com,   Red Hat JBoss Application Server 7.1 on dcpp200q
            df_view['SS_Name'] = df_view['SS_Name'].str.split(' ').str[-1]
            df_view['SS_Name'] = df_view['SS_Name'].str.split('.').str[0]
            
            # drop entries where app_id is null
            df_view = df_view[df_view['App_Asset_ID_'].notna()]

            pge_middleware_list = [
                "PGE_Middleware for API Management",
                "PGE_Middleware for ETL (Extract, Transform, Load)",
                "PGE_Middleware Web Services",
                "PGE_Message-Oriented Middleware (MOM)",
                "PGE_Middleware for Security",
                "PGE_Middleware for Business Process Automation",
                "PGE_Middleware for Enterprise Applications",
                "PGE_Middleware for Real-Time Analytics",
                "PGE_Middleware Tomcat Application Server"
            ]
            # select only thoe entries where SS_System_Role match the pge_middleware_list
            df_view = df_view[df_view['SS_System_Role'].isin(pge_middleware_list)]

            # Add a parsed version column to your dataframe
            df_view['parsed_version'] = df_view['SS_Version_Number'].apply(extract_version_tuple)

            # For each SS_Name, keep only the row with the latest version
            df_view = df_view.loc[df_view.groupby('SS_Name')['parsed_version'].idxmax()]
            
            df_view['SS_Version_Short'] = df_view['SS_Version_Number'].str.split(".").str[:2].str.join(".")
            
            df_view.drop(columns={'parsed_version'}, inplace=True)



        
        elif view_type == 'view_itassets':
            # 1) Convert text to datetime (auto-detect formats; set dayfirst=True if your data is D/M/Y)
            df_view["CS_Installation_Date"] = pd.to_datetime(df_view["CS_Installation_Date"], errors="coerce", dayfirst=True)

            # 2) Add 5 years using DateOffset (handles leap-day sensibly: 2020-02-29 → 2025-02-28)
            df_view["Assumed HW Expiration Date"] = df_view["CS_Installation_Date"] + pd.DateOffset(years=5)

        elif view_type == 'view_database_assets':
            # 1. Extract only the numeric/version part
            df_view['DB_Version_Number'] = df_view['DB_Version_Number'].str.extract(r'(\d[\d\.]*)', expand=False)
            # 2. Keep only the first two components (major.minor)
            df_view['DB_Version_Short'] = df_view['DB_Version_Number'].str.split(".").str[:2].str.join(".")
            # 3. Convert to numeric
            df_view['DB_Version_Short'] = pd.to_numeric(df_view['DB_Version_Short'], errors='coerce')
            
            # Exclude some of the default dbs from the list
            exclude_dbs = ['master' ,'tempdb' ,'model' ,'msdb' ,'DBA']
            df_view = df_view[~df_view['DB_Short_Description'].isin(exclude_dbs)]

            # Exclude all the DB_AssetLifecycleStatus other than ['Deployed','Missing','Down']
            df_view = df_view[df_view['DB_AssetLifecycleStatus'].isin(['Deployed','Missing','Down'])]


        elif view_type == 'view_itassets_managed_services':
            # filter the data
            df_view = df_view[(df_view['CS_AssetLifeCycleStatusName'].isin(['Deployed', 'Missing', 'Down'])) & (df_view['CS_NERCType'] == 5000) & (df_view['CS_Primary_Capability'] == 14)]
            # apply the modification on ss_ids
            df_view = mod_list_col(df_view, 'ss_ids')
            df_view = mod_list_col(df_view, 'db_ids')
            df_view = mod_list_col(df_view, 'cluster_ids')

            # 1) Convert text to datetime (auto-detect formats; set dayfirst=True if your data is D/M/Y)
            df_view["CS_Installation_Date"] = pd.to_datetime(df_view["CS_Installation_Date"], errors="coerce", dayfirst=True)

            # 2) Add 5 years using DateOffset (handles leap-day sensibly: 2020-02-29 → 2025-02-28)
            df_view["Assumed HW Expiration Date"] = df_view["CS_Installation_Date"] + pd.DateOffset(years=5)

        elif view_type == 'view_database_managed_services':
            # apply the modification 
            df_view = mod_list_col(df_view, 'cluster_ids')

            # 1. Extract only the numeric/version part
            df_view['DB_Version_Number'] = df_view['DB_Version_Number'].str.extract(r'(\d[\d\.]*)', expand=False)
            # 2. Keep only the first two components (major.minor)
            df_view['DB_Version_Short'] = df_view['DB_Version_Number'].str.split(".").str[:2].str.join(".")
            # 3. Convert to numeric
            df_view['DB_Version_Short'] = pd.to_numeric(df_view['DB_Version_Short'], errors='coerce')
            
            # Exclude some of the default dbs from the list
            exclude_dbs = ['master' ,'tempdb' ,'model' ,'msdb' ,'DBA']
            df_view = df_view[~df_view['DB_Short_Description'].isin(exclude_dbs)]
            # Exclude all the DB_AssetLifecycleStatus other than ['Deployed','Missing','Down']
            df_view = df_view[df_view['DB_AssetLifecycleStatus'].isin(['Deployed','Missing','Down'])]
            


        return df_view
    except Exception as e:
        logger.info(f'Something went wrong while transforming amps data:{e}')
        send_failure_email('transform_amps_data', 'Something went wrong while transforming amps data')
    

# Transform avamar & ppdm data
def transform_avamar_ppdm_data(df, server_type = 'avamar'):
    try:
        logger.info(f'Transforming {server_type} data Initialized...')
        # remove all the extra text from clent other than servernames
        df['Client'] = df['Client'].str.extract(r'^([A-Za-z0-9]+)')

        
        logger.info(f"Transforming {server_type} Data Completed.")

        # return 
        return df
    except Exception as e:
        logger.info(f'Something went wrong while transforming {server_type} data:{e}')
        send_failure_email('transform_avamar_ppdm_data', f'Something went wrong while transforming {server_type} data')


def merge_storage_data(aiops_df, ibm_df):

    # rename the columns in ibm_df as both dataframe should have same column names
    ibm_df = ibm_df.rename(columns={'Name':'System Name', 'Usable Capacity':'Capacity', 'Used Capacity':'Allocated', 'Availabe Capacity':'Free'})

    # concatenate both the dataframe
    df = pd.concat([aiops_df, ibm_df], ignore_index=True)

    return df


def transform_n_load_master_eols(db_username, db_password, db_name, db_host, db_port):

    # Fetch CI Name, Serial Number, and Host from the master table
    logger.info('Fetching data from master table')
    query = 'SELECT [CI Name],[Serial Number],[Host] From EOSLdatastore.dbo.master_eosl;'
    table_df = fetch_table_data(query, db_username, db_password, db_name, db_host, db_port)

    # Identify IBM frame records based on the CI Name pattern (e.g., 9043-MRX-78FB9CX-P27B08-Kittle)
    pattern = r"^\d{4}-[A-Z]{3}-"
    df_filtered = table_df[table_df['CI Name'].str.match(pattern, na=False)]

    # Extract the set of serial numbers that belong to IBM frames
    valid_serials =  set(df_filtered['Serial Number'])
    
    # Extract the set of IBM frame names from CI Name
    valid_frames = set(df_filtered['CI Name'])
    
    # Map IBM frame serial numbers to their corresponding frame names
    frames = {}
    for frame in valid_frames:
        s_num = frame.split('-')[2]
        frames[s_num] = frame

    # Flag rows where the CI entry represents an IBM frame
    table_df['is_ibm_frame'] = table_df['Serial Number'].isin(valid_serials)

    # Map each serial number to its IBM frame name
    table_df['ibm_frame'] = table_df['Serial Number'].map(frames)

    # Flag rows where the CI entry represents a host
    table_df['is_host'] = table_df['Host'].notna()

    # Determine the HW Asset value: 
    # - If it's a host, use Host
    #  # - If it's an IBM frame, use the IBM frame name
    #  # - Otherwise, default to CI Name
    table_df['HW Asset'] = np.select(
        [
            table_df['is_host'] == True,
            table_df['is_ibm_frame'] == True
        ],
        [
            table_df['Host'],
            table_df['ibm_frame']
        ],
        default = table_df['CI Name']
    )

    # Create a temporary table in db to join with master table
    logger.info('Creating Temporary HW Asset table....')
    load_data_into_db(table_df, 'Temp_HW_Asset', db_username, db_password, db_name, db_host, db_port)
    # Create Index on CI Name
    ## Alter  column with change in its length
    alter_col_len('Temp_HW_Asset', 'CI Name', db_username, db_password, db_name, db_host, db_port, 255)
    create_index_wo_cgl('Temp_HW_Asset', 'CI Name', db_username, db_password, db_name, db_host, db_port)

    alter_col_len('master_eosl', 'CI Name', db_username, db_password, db_name, db_host, db_port, 255)
    create_index_wo_cgl('master_eosl', 'CI Name', db_username, db_password, db_name, db_host, db_port)

    # Add the new columns to the master table (columns from Temp_HW_Asset)
    add_col_query = """
        ALTER TABLE EOSLdatastore.dbo.master_eosl
        ADD [is_host] FLOAT,
            [ibm_frame] VARCHAR(MAX),
            [is_ibm_frame] FLOAT,
            [HW Asset] VARCHAR(MAX)
            ;
        """
    ## run query to add new column to our master table
    logger.info('Adding new columns to the master table...')
    run_custom_query(add_col_query, db_username, db_password, db_name, db_host, db_port)

    # Now Join the data from Temp HW Asset table to master table
    join_query = """
                UPDATE m
                SET 
                    m.[is_host] = t.[is_host],
                    m.[ibm_frame] = t.[ibm_frame],
                    m.[is_ibm_frame] = t.[is_ibm_frame],
                    m.[HW Asset] = t.[HW Asset]

                FROM EOSLdatastore.dbo.master_eosl m
                JOIN EOSLdatastore.dbo.Temp_HW_Asset t
                    ON m.[CI Name] = t.[CI Name];

                """
    
    ## run query to join data to our master table
    logger.info("Putting data from Temp HW Assets to the master table's new columns...")
    run_custom_query(join_query, db_username, db_password, db_name, db_host, db_port)

    # # Now drop the temporary table - Temp_HW_Asset
    # drop_query = "DROP TABLE EOSLdatastore.dbo.Temp_HW_Asset;"
    # ## run query to drop temp table
    # logger.info("Droping Temp HW Assets table...")
    # run_custom_query(drop_query, db_username, db_password, db_name, db_host, db_port)


    


    

    



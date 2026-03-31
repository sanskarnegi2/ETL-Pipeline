import re

import numpy as np
import pandas as pd
import json
import logging

from pygments.token import Name
from src.utils import convert_into_tb, extract_version_tuple, send_failure_email, mod_list_col
from src.load import fetch_table_data, load_data_into_db, run_custom_query, create_index, create_index_wo_cgl, alter_col_len, create_index_w_len

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
def transform_avamar_ppdm_data(df, server_type = 'avamar_server'):
    try:
        if server_type in ['avamar_servers','ppdm_servers']:
            logger.info(f'Transforming {server_type} data Initialized...')
            # remove all the extra text from clent other than servernames
            df['Client'] = df['Client'].str.extract(r'^([A-Za-z0-9]+)')
        
        elif server_type == 'dpa_storage':
            # load eosl data to have the eosl dates for our server
            eosl_df = pd.read_excel('data/raw/dpa_eosl.xlsx')

            # create the separate column s_name that store all the datadomain without their domain in both dataframe
            eosl_df['s_name'] = eosl_df['System Name'].str.split('.').str[0]
            df['s_name'] = df['Hostname'].str.split('.').str[0]

            # merge both dataframe on eosl servers to have eosl dates
            df = pd.merge(
                            df,
                            eosl_df.loc[:, ["s_name", "Dell's Planned year to remediate"]],
                            on='s_name',
                            how = 'left'
                        )

            # Now drop the s_name column
            df.drop(columns=['s_name'], inplace=True)
            # rename the eosl column name
            df.rename(columns={"Dell's Planned year to remediate":"dpa_eosl"}, inplace=True)

        
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
    logger.info('Transforming Master EOSL data for HW Asset, Initialized...') 
    # Fetch CI Name, Serial Number, and Host from the master table
    logger.info('Fetching data from master table')
    query = 'SELECT [CIName_Correct],[Product Name],[Serial Number],[Host], [Operating System], [Application Ids],[Application Names],[Client Owner],[IT Director],[IT Lead],[IT SME],[IT SME Backup],[Managed By],[BIA Tier] From EOSLdatastore.dbo.master_eosl;'
    table_df = fetch_table_data(query, db_username, db_password, db_name, db_host, db_port)

    # Identify IBM frame records based on the CI Name pattern (e.g., 9043-MRX-78FB9CX-P27B08-Kittle)
    pattern = r"^\d{4}-[A-Z]{3}-"
    df_filtered = table_df[table_df['CIName_Correct'].str.match(pattern, na=False)]

    # Extract the set of serial numbers that belong to IBM frames
    valid_serials =  set(df_filtered['Serial Number'])
    
    # Extract the set of IBM frame names from CI Name
    valid_frames = set(df_filtered['CIName_Correct'])
    
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

    # use all the regex pattern on the Product Name to have desired device
    # Frames (IBM Power)
    pt1 = r"^[0-9]{4}-[0-9A-Z]{3}$"
    # vxrail
    pt2 = r"\bVxRail\b"
    # PowerEdge
    pt3 = r"\bPowerEdge\b"
    # Apollo
    pt4 = r"\bApollo\b"
    # IBM System Family
    pt5 = r"^(System\b)|\bIBM System\b"
    # UCS Family
    pt6 = r"\b(UCS|Cisco|SNS)\b"
    # Sun Family
    pt7 = r"\bT5240\b|\bsun ?fire\b|\bsun ?server\b"
    # Kontron
    pt8 = r"\bKontron\b"
    # ConnectPort
    pt9 = r"\bConnectport\b" 
    # SEL family
    pt10 = r"\bSEL-"

    # Combine all patterns into a single regex pattern
    combined_pattern = f"({pt1}|{pt2}|{pt3}|{pt4}|{pt5}|{pt6}|{pt7}|{pt8}|{pt9}|{pt10})"

    # Determine the HW Asset value: 
    # - If it's a host, use Host
    #  # - If it's an IBM frame, use the IBM frame name
    #  # - If pattern match on product name, use the CI Name as HW asset
    #  # - Otherwise, default to None
    table_df['HW Asset'] = np.select(
        [
            table_df['is_host'] == True,
            table_df['is_ibm_frame'] == True,
            table_df['Product Name'].str.contains(combined_pattern, regex=True, flags=re.IGNORECASE, na=False)
        ],
        [
            table_df['Host'],
            table_df['ibm_frame'],
            table_df['CIName_Correct']
        ],
        default = None
    )

    # create new column HW Asset Short by having only the first part of the HW Asset column before any ., as we want to use this column for our analysis and it is better to have only the main asset name in this column instead of having all the details in it.
    table_df['HW Asset Short'] = table_df['HW Asset'].str.split('.').str[0]

    # modifying the operating systme colunm to have the updated_os column with only windows version without edition and other details, as we want to use this column for our analysis and it is better to have only windows version in this column instead of having all the details in it.
    # pattern = r"(Windows(?:\s+\w+)*?(?:\s+\d[\w.]*)?)(?=\s+(Standard|Datacenter|Professional|Enterprise|Essentials|Pro|Home|Education|Core)\b|$)"
    pattern = r"(Windows(?:\s+\w+)*?(?:\s+\d[\w.]*)?)(?=\s*(Standard|Datacenter|Professional|Enterprise|Essentials|Pro|Home|Education|Core|\(|$))"

    mask = table_df["Operating System"].str.contains("Windows", case=True, na=False)

    # new_df.loc[mask, "updated_os"] = new_df.loc[mask, "Operating System"].str.extract(pattern)[0]
    table_df["updated_os"] = table_df["Operating System"].where(~mask, table_df["Operating System"].str.extract(pattern)[0])

        
    # Create a temporary table in db to join with master table
    logger.info('Creating Temporary HW Asset table....')
    load_data_into_db(table_df, 'Temp_HW_Asset', db_username, db_password, db_name, db_host, db_port)
    # Create Index on CI Name
    ## Alter  column with change in its length
    create_index_w_len('Temp_HW_Asset', 'CIName_Correct', db_username, db_password, db_name, db_host, db_port, 255)
    
    create_index_w_len('master_eosl', 'CIName_Correct', db_username, db_password, db_name, db_host, db_port, 255)

    # Add the new columns to the master table (columns from Temp_HW_Asset)
    add_col_query = """
        ALTER TABLE EOSLdatastore.dbo.master_eosl
        ADD [is_host] FLOAT,
            [ibm_frame] VARCHAR(MAX),
            [is_ibm_frame] FLOAT,
            [HW Asset] VARCHAR(MAX),
            [HW Asset Short] VARCHAR(MAX),
            [updated_os] VARCHAR(MAX)
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
                    m.[HW Asset] = t.[HW Asset],
                    m.[HW Asset Short] = t.[HW Asset Short],
                    m.[updated_os] = t.[updated_os]

                FROM EOSLdatastore.dbo.master_eosl m
                JOIN EOSLdatastore.dbo.Temp_HW_Asset t
                    ON m.[CIName_Correct] = t.[CIName_Correct];

                """
    
    ## run query to join data to our master table
    logger.info("Putting data from Temp HW Assets to the master table's new columns...")
    run_custom_query(join_query, db_username, db_password, db_name, db_host, db_port)

    # # Now drop the temporary table - Temp_HW_Asset
    # drop_query = "DROP TABLE EOSLdatastore.dbo.Temp_HW_Asset;"
    # ## run query to drop temp table
    # logger.info("Droping Temp HW Assets table...")
    # run_custom_query(drop_query, db_username, db_password, db_name, db_host, db_port)

    # # Also return the trasformed table as dataframe
    # query = "SELECT * From EOSLdatastore.dbo.master_eosl;"
    # transformed_master_df = fetch_table_data(query, db_username, db_password, db_name, db_host, db_port)
    # return transformed_master_df


def normalize_multi_value_columns(db_username, db_password, db_name, db_host, db_port):

    # Fetch master table data to have the multi value columns data for normalization and splitting
    logger.info('Fetching data from master table')
    query = 'SELECT * From EOSLdatastore.dbo.master_eosl;'
    df = fetch_table_data(query, db_username, db_password, db_name, db_host, db_port)

    
    # Columns that contain semicolon-delimited multi‑value fields
    cols_to_split = [
        "Application Ids", "Application Names", "Client Owner", "IT Director",
        "IT Lead", "IT SME", "IT SME Backup", "Managed By", "BIA Tier"
    ]

    # -----------------------------------------------------------
    # STEP 1 — Normalize and split all multi-value columns
    # -----------------------------------------------------------
    df[cols_to_split] = (
        df[cols_to_split]
        .fillna("")                     # Replace NaN/None with empty string
        .astype(str)                    # Ensure all values are strings
        .apply(lambda s: 
            s.str.split(r';\s*')        # Split on semicolon + optional spaces
        )                               # IMPORTANT: s is a Series → regex works
    )

    # -----------------------------------------------------------
    # STEP 2 — Compute the maximum list length per row
    # This tells us how many items the row *should* have
    # -----------------------------------------------------------
    df["max_len"] = df[cols_to_split].applymap(len).max(axis=1)

    # -----------------------------------------------------------
    # STEP 3 — Pad shorter lists so all columns have equal length
    # If a column has only 1 value but others have 2 or 3,
    # we repeat the single value to match the longest list.
    # -----------------------------------------------------------
    def pad_lists(row):
        for col in cols_to_split:
            current_len = len(row[col])             # How many items this column has
            needed = row["max_len"] - current_len   # How many more items needed
            if needed > 0:
                # Repeat the first value to fill the gap
                row[col] = row[col] + [row[col][0]] * needed
        return row

    df = df.apply(pad_lists, axis=1)

    # -----------------------------------------------------------
    # STEP 4 — Explode all columns together
    # Now that all lists have matching lengths, explode works safely.
    # -----------------------------------------------------------
    df = df.explode(cols_to_split, ignore_index=True)

    # -----------------------------------------------------------

    ## drop DB EOSL column as it is not required and also it is creating duplicate rows because of multiple dbs for same server
    # Step 1: Drop the column
    df = df.drop(columns=["DB EOSL", "OS Expiry Date", "MW EOSL"])

    # Step 2: Keep only unique rows
    df = df.drop_duplicates()


    # Also return the trasformed table as dataframe
    return df



def merge_dpa_data(db_username, db_password, db_name, db_host, db_port,server='ppdm'): # Merge UDN and ODN data for DPA Servers

    # Fetch CI Name, Serial Number, and Host from the master table
    logger.info(f'Fetching data from dpa table for {server} servers...')
    query =f'SELECT * From EOSLdatastore.dbo.{server}_servers_udn;'
    dpa_udn = fetch_table_data(query, db_username, db_password, db_name, db_host, db_port)
    query =f'SELECT * From EOSLdatastore.dbo.{server}_servers_odn;'
    dpa_odn = fetch_table_data(query, db_username, db_password, db_name, db_host, db_port)
    # only take columns that are common in both dataframe for merging
    common_cols = list(set(dpa_udn.columns).intersection(set(dpa_odn.columns)))
    dpa_udn = dpa_udn[common_cols]  
    dpa_odn = dpa_odn[common_cols]
    # concatenate both the dataframe
    merged_df = pd.concat([dpa_udn, dpa_odn], ignore_index=True)
    
    # load the merged data into new table in db
    load_data_into_db(merged_df, f'{server}_servers', db_username, db_password, db_name, db_host, db_port)
    # creating indexes on Client for avamar_servers table
    create_index_w_len(f'{server}_servers', 'Client',db_username, db_password, db_name, db_host, db_port, 255)
    



    

    



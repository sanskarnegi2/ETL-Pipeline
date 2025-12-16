import os
import logging
import asyncio
import aiohttp
import time
import numpy as np
import pandas as pd
import warnings
from io import StringIO
from dotenv import load_dotenv
from pandas import json_normalize
from src.utils import get_vrops_auth_token, get_amps_auth_token, convert_lists_to_json, get_dpa_token, create_session_with_retries
from src.utils import remove_duplicate_cols, get_aiops_auth_token, get_ibm_auth_token
from src.extract import get_vrops_identifiers, run_vrops_extraction, get_amps_view_names, fetch_amps_data, fetch_ddboost_data
from src.extract import get_node_id, get_report_url, get_dpa_report, fetch_nas_data, fetch_aiops_data, fetch_ibm_data
from src.transform import flatten_vrops_data, transform_vmware_data, transform_esxi_data, transform_nas_data
from src.transform import transform_aiops_data, transform_ibm_data, transform_amps_data
from src.load import load_vmware_data_into_db, load_amps_data_into_db, run_custom_query, create_index
# Local application imports from config.py
from config import vmware_metrics_names, esxi_metrics_names, vmware_properties_names, esxi_properties_names
from config import  vmware_column_mapping, esxi_column_mapping, vmware_create_table_query, esxi_create_table_query
from config import  vmware_insert_sql_query, esxi_insert_sql_query
from config import avamar_list, ppdm_list, nas_file_paths, ddboost_host


# Configure logging to write to a file
logger = logging.getLogger(__name__)
# setup loggers
logging.basicConfig(
    handlers=[
        logging.FileHandler('logs/etl.log', mode='a'),
        logging.StreamHandler()
    ],
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
    )

logger.info("ETL process started")



# filter warnings
warnings.filterwarnings('ignore')

# get the username password(from .env)
load_dotenv()
vrops_uname = os.getenv("VOPS_UNAME")
svc_pwd = os.getenv("SVC_PWD")
svc_uname = os.getenv("SVC_UNAME")
dell_pwd = os.getenv("DELL_PWD")
# get DB info
db_username = os.getenv("SVC_UNAME")
db_password = os.getenv("SVC_PWD")
db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

# get AIOPS info
aiops_client_id = os.getenv("AIOPS_CLIENT_ID")
aiops_client_secret = os.getenv("AIOPS_CLIENT_SECRET")
ibm_tenant_id = os.getenv("DELL_TENANT_ID")
ibm_api_key = os.getenv("DELL_API_KEY")


# URls
vrops_host = 'https://vcf-mgt-vrops.utility.pge.com/'
vrops_auth_url =  f'{vrops_host}/suite-api/api/auth/token/acquire'
amps_login_url = "https://amps.cloud.pge.com/axe-platform/login"
amps_portal_url = "https://amps.cloud.pge.com/axe-platform/portal"
aiops_auth_url = 'https://apigtwb2c.us.dell.com/auth/oauth/v2/token'
ibm_auth_url  = f"https://insights.ibm.com/restapi/v1/tenants/{ibm_tenant_id}/token"
ddboost_script_path = "/tsm_ops/admin/eosl_ddboost_data_collect.sh"
ddboost_script_output_path = "/tsm_ops/admin/eosl_ddboost_list.csv"
eosl_asset_file_path = "data/raw/Component_Category_COMC_554__Windows.xlsx"
storage_analysis_file_path = 'data/raw/ru_count_storage.xlsx'

# local variables
# amps_view_list = ['view_applications', 'view_database_assets', 'view_itassets', 'view_middleware_assets']
amps_view_list = ['view_database_assets']

## Using other variables from config.py


# Get and load the VirtualMachine data into database table
def load_vmware_data(vrops_token, vrops_host, vmware_metrics_names, vmware_properties_names, vmware_column_mapping, db_username, db_password, db_name, db_host, db_port):
    # get the identifiers for the VMware(vROps)
    vmware_ids = get_vrops_identifiers(vrops_token, vrops_host, resourceKind='VirtualMachine')

    # fetch metrics and properties for VMWARE (ids)
    vmware_data = asyncio.run(run_vrops_extraction(vrops_token, vmware_ids, vrops_host, vmware_metrics_names, 40, 'VirtualMachine'))

    # Allow cleanup to finish
    asyncio.sleep(1)

    # flatten the result(properties, metrics) into dictionary
    flatten_vmware_data = flatten_vrops_data(vmware_properties_names, vmware_data, 'VirtualMachine')

    # transform and get vmware data as DataFrame
    df_vmware = transform_vmware_data(flatten_vmware_data, vmware_column_mapping)

    # load vmware data into mysql server database
    load_vmware_data_into_db(df_vmware, db_username, db_password, db_name, db_host, db_port, vmware_create_table_query, vmware_insert_sql_query)



# Get and load the ESXi Host data into database table
def load_esxi_data(vrops_token, vrops_host, esxi_metrics_names, esxi_properties_names, esxi_column_mapping, db_username, db_password, db_name, db_host, db_port):
    # get the identifiers for the ESXi Host(vROps)
    esxi_ids = get_vrops_identifiers(vrops_token, vrops_host, resourceKind='HostSystem')

    # fetch metrics and properties for VMWARE (ids)
    esxi_data = asyncio.run(run_vrops_extraction(vrops_token, esxi_ids, vrops_host, esxi_metrics_names, 40, 'HostSystem'))

    # Allow cleanup to finish
    asyncio.sleep(0.5)

    # flatten the result(properties, metrics) into dictionary
    flatten_esxi_data = flatten_vrops_data(esxi_properties_names, esxi_data, 'HostSystem')

    # transform and get vmware data as DataFrame
    df_esxi = transform_esxi_data(flatten_esxi_data, esxi_column_mapping)

    # load vmware data into mysql server database
    load_vmware_data_into_db(df_esxi, db_username, db_password, db_name, db_host, db_port, esxi_create_table_query, esxi_insert_sql_query)

    # create index on the SD Name column in table
    create_index(table='ESXi', column='SD_Name', user=db_username, password=db_password, db_name=db_name, host=db_host, port=db_port)


# Get and load the AMPs data into database table
def load_amps_data(token, view_type, db_username, db_password, db_name, db_host, db_port):
    try:
        start_time = time.time() 
        # fetch amps data
        all_data = fetch_amps_data(token, view_type, 0, 1000)
        time.sleep(1)
        
        if all_data:
            # make dataframe from the data, once all data fetched
            df_view = json_normalize(all_data)
            
            # convert list type columns into json for databse compatibality
            df_view = convert_lists_to_json(df_view)
            time.sleep(1)

            # Trasnsform AMPs data
            df_view = transform_amps_data(df_view, view_type)

            # save the data as excel file
            # df_view.to_excel(f'data/processed/{view_type}.xlsx', index=False)


            # load data into database
            load_amps_data_into_db(df_view, view_type, db_username, db_password, db_name, db_host, db_port)
            
            # end_time
            end_time = time.time() - start_time
            logger.info(f'Time Taken to fetch and load data for {view_type}: {end_time}')
        else:
            logger.info('Something Went Wrong.')
            return
    except Exception as e:
        logger.info(f'Error while loading amps data for {view_type}: {e}')

# Get and load the DPA data into database table
def load_dpa_data(token, query_values: list, server='avamar_servers'):
    # create a list to store all reports
    all_reports = []
    all_node_ids = [] 
    # Iterate through all the query_values
    for query_value in query_values:
        # create session
        session = create_session_with_retries()
        # Step 1: Get node ID
        node_ids = get_node_id(token, session, query_value)
        time.sleep(2)
        
        if node_ids:
            all_node_ids.extend(node_ids)
        else:
            logger.info(f"Node IDs not found for :{query_value}")            
        
    # only go ahead if having node ids
    if not all_node_ids:
        logger.info(f"No node ID found for {query_values}")
        return None
    else:
        logger.info(f'Generated node ids: {all_node_ids}')
        logger.info(f"total node ids: {len(all_node_ids)}")

    # generate the report urls for the node_ids
    report_urls = get_report_url(token, session, all_node_ids)
    logger.info(f"All report urls: {report_urls}")
    
    # only go ahead if having report_urls
    if not report_urls:
        logger.info(f"No report URL generated for node IDs: {node_ids}")
        return None

    
    # Step 3: Get DPA report content
    dfs = []
    for retry in range(2): # so that it will retry for non fetched servers
        print('try:',retry+1)
        # create session
        session = create_session_with_retries()
        dpa_reports = get_dpa_report(token, session, report_urls)
        time.sleep(1)

        if not dpa_reports:
            logger.info(f"No report content retrieved from URLs: {report_urls}")
            return None
        # Extend fetched reports into all_reports
        all_reports.extend(dpa_reports)
    
        # Step 4: Convert CSV string to DataFrame
        try:
            for dpa_report in all_reports: 
                df = pd.read_csv(StringIO(dpa_report))
                dfs.append(df)
            logger.info("Report successfully converted to DataFrame.")

            # Final dataframe
            final_df = pd.concat(dfs, ignore_index=True)
            # logger.info(f'Total rows fetched: {final_df.shape[0]}')
            
            # logging to check which server is not fetched
            # get unique servers from generated report
            fetched_servers = final_df['Server'].unique()
            unfetched_servers = [i for i in query_values if i not in fetched_servers]
            logger.info(f'Unfetched servers: {unfetched_servers}')
            
            # if its our first try and also having some non fetched servers, we will retry and use only non fetched report urls instead of all
            if unfetched_servers: 
                report_urls = [i for i in report_urls if i['query_value'] in unfetched_servers]
                logger.info(f'Unfetched reports in {retry+1} try for URLs: {report_urls}')
            else:
                # if all server fetched, do not retry
                break

        except Exception as e:
            logger.info(f"Failed to parse report CSV: {e}")
            return None
    
    # Step 5 load data into databae table
    load_amps_data_into_db(final_df, server, db_username, db_password, db_name, db_host, db_port)
    
# Get and load the NAS data into database table
def load_nas_data(username, password, file_paths, domain='PGE', table_name='nas_report'):
    # fetch nas data (list of dataframes NAS) 
    dataframes = fetch_nas_data(username, domain, password, file_paths)
    # load master excel file as df to do Vlookup
    master_df = pd.read_excel('data/raw/NAS/NAS Master sheet.xlsx')

    nas_data_df = transform_nas_data(dataframes, master_df)

    # before loading into db, save it as excel file
    nas_data_df.to_excel('data/processed/nas_data.xlsx', index=False)


    # load data into databae table
    load_amps_data_into_db(nas_data_df, table_name, db_username, db_password, db_name, db_host, db_port)


# Get and load the SAN data into database table
def load_san_data(aiops_token, ibm_token, ibm_tenant_id, table_name='san_report'):
    # fetch aiops data  
    aiops_df = fetch_aiops_data(aiops_token)
    # fetch ibm data  
    ibm_df = fetch_ibm_data(ibm_token, ibm_tenant_id)
    # open SAN Master excel file as dataframe
    master_df = pd.read_excel('data/raw/SAN/SAN Master.xlsx')
    # transform aiops_df
    merged_aiops = transform_aiops_data(aiops_df, master_df)
    # transform ibm_df
    merged_ibm = transform_ibm_data(ibm_df, master_df)
    # merge both the transformed reports 'merged_aiops_df' 'merged_ibm_df'
    # Merge the two DataFrames on all shared columns
    san_df = pd.merge(
        merged_aiops,
        merged_ibm,
        on=['StorageGroupName', 'ServerName', 'SystemDisplayName', 'APP -ID', 'Application Name', 'Total Size (TB)', 'Used (TB)'],
        how='outer'
    )

    # before loading into db, save it as excel file
    san_df.to_excel('data/processed/san_data.xlsx', index=False)

    # load data into databae table
    load_amps_data_into_db(san_df, table_name, db_username, db_password, db_name, db_host, db_port)

    # create index on the SystemDisplayName column in table
    create_index(table_name,'SystemDisplayName', db_username, db_password, db_name, db_host, db_port)
    

def load_ddboost_data(hostname, port, username, password, script_path, output_path, table_name='ddboost_report'):
    # fetch ddboost data
    ddboost_df = fetch_ddboost_data(hostname, port, username, password, script_path, output_path)
    # load into the database table
    load_amps_data_into_db(ddboost_df, table_name, db_username, db_password, db_name, db_host, db_port)

def load_eosl_aaset(file_path, db_username, db_password, db_name, db_host, db_port, table_name = 'EOSL_assets'):
    try:
        # Load all sheets into a dictionary of DataFrames
        # all_sheets = pd.read_excel('data/raw/Component_Category_COMC_554__Windows.xlsx', sheet_name=None)
        all_sheets = pd.read_excel(file_path, sheet_name=None)

        # Concatenate all DataFrames into one
        merged_df = pd.concat(all_sheets.values(), ignore_index=True)

        # Transformation (creatin new column from existing one)
        merged_df['Short_Version'] = merged_df['Version'].str.split(".").str[:2].str.join(".")

        
        # load into database
        load_amps_data_into_db(merged_df, table_name, db_username, db_password, db_name, db_host, db_port)

    except Exception as e:
        logger.info('Something went wrong while reuploading the EOSL Assets file in Database')
        logger.info(e)

def load_storage(file_path, db_username, db_password, db_name, db_host, db_port, table_name = 'storage_analysis'):
    try:
        # load the sheet as dataframe
        storage_df = pd.read_excel(file_path)

        # load into database
        load_amps_data_into_db(storage_df, table_name, db_username, db_password, db_name, db_host, db_port)
    except Exception as e:
        logger.info('Something went wrong while reuploading the storage analysis file in Database')
        logger.info(e)



if __name__ == "__main__":
    # get the token for vROps
    vrops_token = get_vrops_auth_token(vrops_uname, svc_pwd, vrops_auth_url)
    
    logger.info('Initialize data fetching and loading into database for VirtualMachine')
    load_vmware_data(vrops_token, vrops_host, vmware_metrics_names, vmware_properties_names, vmware_column_mapping, db_username, db_password, db_name, db_host, db_port)
    
    # get the token for vROps (We Twice fetched the token, as we dont know the expiry of token)
    vrops_token = get_vrops_auth_token(vrops_uname, svc_pwd, vrops_auth_url)

    logger.info('Initialize data fetching and loading into database for ESXi Host')
    load_esxi_data(vrops_token, vrops_host, esxi_metrics_names, esxi_properties_names, esxi_column_mapping, db_username, db_password, db_name, db_host, db_port)

    # Fetch & Load data for desired view_types of AMPs, i.e. view_list = ['view_applications', 'view_database_assets', 'view_it_assets']
    for view_type in amps_view_list:
        # get token for AMPs
        amps_token = get_amps_auth_token(svc_uname, svc_pwd, amps_login_url, amps_portal_url)

        logger.info(f'Initialize data fetching and loading into database for AMPs: {view_type}')
        load_amps_data(amps_token, view_type, db_username, db_password, db_name, db_host, db_port)
    
    ## Fetch & Load data for DPA
    # get dpa-token
    dpa_token = get_dpa_token(svc_uname, dell_pwd)
    logger.info('Initialize data fetching and loading into database for Avamar Server')
    load_dpa_data(dpa_token, avamar_list, 'avamar_servers')
    logger.info('Initialize data fetching and loading into database for PPDM Server')
    load_dpa_data(dpa_token, ppdm_list, 'ppdm_servers')

    # load nas data
    load_nas_data(username=svc_uname, password=svc_pwd, file_paths=nas_file_paths, domain='PGE', table_name='nas_report')
    
    # load san data
    # get the token for AIOPS
    aiops_token = get_aiops_auth_token(aiops_client_id, aiops_client_secret, aiops_auth_url)
    # get the token of Dell
    ibm_token = get_ibm_auth_token(ibm_api_key, ibm_auth_url)

    logger.info('Initialize data fetching and loading into database for SAN storage')
    load_san_data(aiops_token, ibm_token, ibm_tenant_id, 'san_report')
    
    # Load DDBoost Data
    logger.info('Initialize data fetching and loading into database for DDBoost report')
    load_ddboost_data(hostname = ddboost_host, port = 22, username = svc_uname, password = svc_pwd, script_path = ddboost_script_path, output_path = ddboost_script_output_path, table_name = 'ddboost_report')
    
    # Load EOSL Assets
    logger.info('Initialize data fetching and loading into database for EOSL Assests')
    load_eosl_aaset(eosl_asset_file_path, db_username, db_password, db_name, db_host, db_port, 'EOSL_assets')

    # Load Storage Analysis
    logger.info('Initialize data fetching and loading into database for Storage Analysis')
    load_storage(storage_analysis_file_path, db_username, db_password, db_name, db_host, db_port, 'storage_analysis')

   
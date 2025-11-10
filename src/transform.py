import numpy as np
import pandas as pd
import json
import logging
from src.utils import convert_into_tb

# setup loggers
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# transform VSphere data
def transform_vsphere_string(st):
    try:
        st = json.loads(st)
        return [f"<{i['category']}-{i['name']}>" for i in st]
    except:
        return "none"

# Convert each VM's property list into dictionary
def flatten_vrops_data(metric_names, results, resourceKind='VirtualMachine'):
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

# Transform Virtual Machine Data
def transform_vmware_data(flatten_vmware_data, vmware_column_mapping):
    logger.info('Start Transforming VirtualMachine data')
    # convert the data into dataframe
    df_vmware = pd.DataFrame(flatten_vmware_data)

    # rename columns
    df_vmware.rename(columns=vmware_column_mapping, inplace=True)
    
    # preserve order
    df_vmware = df_vmware[vmware_column_mapping.values()]

    # apply transformation on vSphere
    df_vmware['Vsphere Tags'] = df_vmware['Vsphere'].apply(transform_vsphere_string)
    df_vmware.drop(columns = ['Vsphere'], inplace=True)

    # Memory in KB, converting in MB
    df_vmware["Memory"] = df_vmware['Memory'].astype(float) / (1024**2)

    # Convert Memory Utilization from GB to TB
    df_vmware['Memory Utilization'] = round(df_vmware['Memory Utilization'].astype(float) / (1024**2), 2)

    # total disk in GB converting into TB
    df_vmware['Total Disk Space'] = round(df_vmware['Total Disk Space'].astype(float) / 1024, 2)

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


# Transform ESXi Host Data
def transform_esxi_data(flatten_esxi_data, esxi_column_mapping):
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


    # Save as Excel file
    # Save the data as excel file
    df_esxi.to_excel('data/processed/new_fetched_esxi.xlsx', index=False)
    logger.info("ESXi Host Data Saved as Excel File")
    
    logger.info("Transforming ESXi Host Data Completed.")

    # return dataframe
    return df_esxi


# Transform NAS data
def transform_nas_data(dataframes, master_df):
    logger.info('Start Transforming NAS data')
    
    # concatenate all the dataframes
    nas_df = pd.concat(dataframes)

    # Convert the Units in TB, Apply the function row-wise
    nas_df['Allocated'] = nas_df.apply(lambda row: convert_into_tb(row['Allocated Size'], row['Allocated Unit']), axis=1)
    nas_df['Used'] = nas_df.apply(lambda row: convert_into_tb(row['Used Size'], row['Used Unit']), axis=1)

    # rename columns for nas df
    nas_df.rename(columns={'APP-IDs from Share Descriptions': 'APP-ID'}, inplace=True)

    # select only required columns from the nas and master dataframe
    nas_df = nas_df[['Path', 'Allocated', 'Used', 'APP-ID']]
    master_df = master_df[['Path', 'APP-ID']]

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

    # Save as Excel file
    # Save the data as excel file
    new_df.to_excel('data/processed/merged_nas_report.xlsx', index=False)
    logger.info("NAS Data Saved as Excel File")
    
    logger.info("Transforming ESXi Host Data Completed.")

    # return dataframe
    return new_df


# Transform AIOPS data
def transform_aiops_data(aiops_df, master_df):
    logger.info('Transforming AIOPS(SAN) data Initialized...')
    # transform data
    aiops_df['Total Size (TB)'] = (aiops_df['total_size']/(1024**4)).round(2)
    aiops_df['Used (TB)'] = (aiops_df['allocated_size']/(1024**4)).round(2)
    aiops_df.drop(columns=['id','allocated_size','total_size'], inplace=True)
    # rename columns
    aiops_df.rename(columns = {"name":"StorageGroupName"}, inplace= True)

    # modify master_df
    master_df.drop(columns=['TotalSize(TB)', 'Used(TB)'],inplace=True)
    merged_aiops_df = pd.merge(aiops_df, master_df,on='StorageGroupName', how='left')
    # save the excel file as well
    merged_aiops_df.to_excel('data/processed/merged_aiops.xlsx', index=False)

    logger.info("Transforming AIOPS(SAN) Data Completed.")

    # return
    return merged_aiops_df


# Transform IBM data
def transform_ibm_data(ibm_df, master_df):
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
    merged_ibm = pd.merge(ibm_df, master_df,on='ServerName', how='left')
    
    # store transform file as excel
    merged_ibm.to_excel('data/processed/merged_ibm.xlsx', index=False)
    
    logger.info("Transforming IBM(SAN) Data Completed.")

    # return 
    return merged_ibm









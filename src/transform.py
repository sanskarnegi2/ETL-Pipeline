import numpy as np
import pandas as pd
import json
import logging

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



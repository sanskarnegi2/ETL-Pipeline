# config.py
# Configuration file for VMware and ESXi metrics extraction and SQL operations.
# It helps keep the main script clean and maintainable by centralizing reusable data,
# This file centralizes metric definitions, property mappings, column headers, and SQL queries
# used throughout the ETL pipeline for clarity and maintainability.

# --------------------------------------------------------------------------------
# Desired Metrics (used while extracting data via run_vrops_extraction -> fetch_metrics)
# --------------------------------------------------------------------------------

## VMware VM Metrics
vmware_metrics_names = [
        'mem|consumed_average',
        'cpu|usage_average',
        'guestfilesystem|usage_total',
        'guestfilesystem|capacity_total'
    ]

## ESXi Host Metrics
esxi_metrics_names = [
    "summary|number_running_vcpus",
    "cpu|usage_average",
    "mem|granted_average",
    "mem|host_usagePct",
    "mem|reservedCapacityPct",
    "mem|overhead_average",
    "sys|uptime_latest"
  ]

# --------------------------------------------------------------------------------
# Desired Properties (used along with metrics for extraction)
# --------------------------------------------------------------------------------

## VMware VM Properties
vmware_properties_names = ['config|name',
 'summary|runtime|powerState',
 'summary|folder',
 'summary|customTag:PGE-AppID|customTagValue',
 'config|version',
 'summary|guest|toolsVersion',
 'summary|guest|fullName',
 'summary|guest|ipAddress',
 'net:4000|mac_address',
 'config|hardware|memoryKB',
 'mem|consumed_average',
 'config|hardware|numCpu',
 'config|hardware|numCoresPerSocket',
 'cpu|usage_average',
 'config|hardware|diskSpace',
 'guestfilesystem|usage_total',
 'guestfilesystem|capacity_total',
 'summary|parentHost',
 'summary|parentCluster',
 'summary|parentVcenter',
 'summary|datastore',
 'summary|customTag:Last Dell PowerProtect Backup|customTagValue',
 'summary|tagJson',
 'summary|customTag:LastSuccessfulBackup-com.dellemc.avamar|customTagValue']


## ESXi Host Properties
esxi_properties_names = ['config|name',
'Certificate Summary|ESXi Host Certificate|End Date',
'summary|parentVcenter', 
'summary|parentDatacenter', 
'summary|parentCluster', 
'net|mgmt_address', 
'summary|version', 
'hardware|vendorModel', 
'hardware|serialNumberTag', 
'runtime|maintenanceState', 
'runtime|connectionState', 
'config|diskSpace', 
'cpu|numCpuSockets', 
'hardware|cpuInfo|numCpuCores', 
'summary|number_running_vcpus', 
'cpu|usage_average', 
'hardware|memorySize', 
'mem|granted_average', 
'mem|host_usagePct', 
'mem|reservedCapacityPct',
'mem|overhead_average', 
'cpu|cpuModel', 
'sys|uptime_latest']

# --------------------------------------------------------------------------------
# Column Mapping Dictionaries (used to rename attributes to sheet headings)
# --------------------------------------------------------------------------------

## VMware Column Mapping
vmware_column_mapping = {
    "config|name": "VM Name",
    "summary|runtime|powerState": "Power State",
    "summary|folder": "VCenter Folder",
    "summary|customTag:PGE-AppID|customTagValue": "App ID",
    "config|version": "VM Hardware Version",
    "summary|guest|toolsVersion": "VM Tools Version",
    "summary|guest|fullName": "Operating System",
    "summary|guest|ipAddress": "Guest IP Address",
    "net:4000|mac_address": "MAC Address",
    "config|hardware|memoryKB": "Memory",
    "mem|consumed_average": "Memory Utilization",
    "config|hardware|numCpu": "Virtual CPU",
    "config|hardware|numCoresPerSocket": "Cores Per Socket",
    "cpu|usage_average": "CPU Usage",
    "config|hardware|diskSpace": "Total Disk Space",
    "guestfilesystem|usage_total": "Disk Utlization (TB)",
    "guestfilesystem|capacity_total": "Disk Capacity",
    "summary|parentHost": "Current Host",
    "summary|parentCluster": "Cluster",
    "summary|parentVcenter": "vCenter",
    "summary|datastore": "Datastore",
    "summary|customTag:Last Dell PowerProtect Backup|customTagValue": "PPDM Backup",
    "summary|tagJson": "Vsphere",
    "summary|customTag:LastSuccessfulBackup-com.dellemc.avamar|customTagValue": "Last Successful Backup"
}

## ESXi Host Column Mapping
esxi_column_mapping = {
    'config|name': "Name",
    'Certificate Summary|ESXi Host Certificate|End Date': "Cert END",
    'summary|parentVcenter': "vCenter",
    'summary|parentDatacenter': "Datacenter",
    'summary|parentCluster': "Cluster",
    'net|mgmt_address': "Mgm IP",
    'summary|version': "ESXi Version",  # Note: This seems mismatched; consider verifying
    'hardware|vendorModel': "HW Model",          # Also seems mismatched
    'hardware|serialNumberTag': "HW Serial no",
    'runtime|maintenanceState': "Maintenance state",
    'runtime|connectionState': "Connection state",
    'config|diskSpace': "Datastore Disk Space",
    'cpu|numCpuSockets': "Socket",
    'hardware|cpuInfo|numCpuCores': "Cores",
    'summary|number_running_vcpus': "vCPU Allocated",
    'cpu|usage_average': "Host CPU Usage %",
    'hardware|memorySize': "Host Memory | GB",
    'mem|granted_average': "Memory Allocated | GB",
    'mem|host_usagePct': "Host Mem Usage %",
    'mem|reservedCapacityPct': "Memory Reserved %",
    'mem|overhead_average': "Memory Overhead",
    'cpu|cpuModel': "CPU Model",
    'sys|uptime_latest': "System|Uptime (Day(s))"
}


# --------------------------------------------------------------------------------
# SQL Queries (used for table creation and data insertion)
# --------------------------------------------------------------------------------

## VMware Table Creation Query
vmware_create_table_query = """
IF OBJECT_ID('dbo.VMware', 'U') IS NOT NULL DROP TABLE dbo.VMware;

CREATE TABLE dbo.VMware (
    [VM Name] NVARCHAR(255),
    [Power State] NVARCHAR(50),
    [VCenter Folder] NVARCHAR(255),
    [App ID] NVARCHAR(255),
    [VM Hardware Version] NVARCHAR(255),
    [VM Tools Version] NVARCHAR(255),
    [Operating System] NVARCHAR(255),
    [Guest IP Address] NVARCHAR(255),
    [MAC Address] NVARCHAR(255),
    [Memory] FLOAT,
    [Memory Utilization] FLOAT,
    [Virtual CPU] NVARCHAR(255),
    [Cores Per Socket] NVARCHAR(50),
    [CPU Usage] FLOAT,
    [Total Disk Space] FLOAT,
    [Disk Utlization (TB)] FLOAT,
    [Disk Capacity Remaining] FLOAT,
    [Current Host] NVARCHAR(255),
    [Cluster] NVARCHAR(255),
    [vCenter] NVARCHAR(255),
    [Datastore] NVARCHAR(255),
    [PPDM Backup] NVARCHAR(500),
    [Last Successful Backup] NVARCHAR(255),
    [Vsphere Tags] NVARCHAR(500)
)
"""

## ESXi Table Creation Query
esxi_create_table_query = """
IF OBJECT_ID('dbo.ESXi', 'U') IS NOT NULL DROP TABLE dbo.ESXi;

CREATE TABLE dbo.ESXi (
    [Name] NVARCHAR(255),
    [Cert END] NVARCHAR(255),
    [vCenter] NVARCHAR(255),
    [Datacenter] NVARCHAR(255),
    [Cluster] NVARCHAR(255),
    [Mgm IP] NVARCHAR(255),
    [ESXi Version] NVARCHAR(255),
    [HW Model] NVARCHAR(255),
    [HW Serial no] NVARCHAR(255),
    [Maintenance state] NVARCHAR(255),
    [Connection state] NVARCHAR(255),
    [Datastore Disk Space] FLOAT,
    [Socket] NVARCHAR(255),
    [Cores] NVARCHAR(255),
    [vCPU Allocated] FLOAT,
    [Host CPU Usage %] FLOAT,
    [Host Memory | GB] FLOAT,
    [Memory Allocated | GB] FLOAT,
    [Host Mem Usage %] FLOAT,
    [Memory Reserved %] FLOAT,
    [Memory Overhead] FLOAT,
    [CPU Model] NVARCHAR(255),
    [System|Uptime (Day(s))] FLOAT
);
"""

## VMware Insert Query
vmware_insert_sql_query = """
INSERT INTO dbo.VMware VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

## ESXi Insert Query
esxi_insert_sql_query = """
INSERT INTO dbo.ESXi VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

## Avamar Server List
avamar_list = ['ffav01.comp.pge.com',
 'rcav01.comp.pge.com',
 'fxav02.comp.pge.com',
 'rcav02.comp.pge.com',
 'fxav03',
 'rcav03']

ppdm_list = [
    "ffppdm01.comp.pge.com",
    "ffppdm02.comp.pge.com",
    "ffppdm06.comp.pge.com",
    "ffppdm07-uiq.comp.pge.com",
    "rcppdm01.comp.pge.com",
    "rcppdm02.comp.pge.com",
    "rcppdm06.comp.pge.com",
    "rcppdm07-uiq.comp.pge.com",
    "rcppdm03",
    "rcppdm04"
]

# NAS shared resource filepath
nas_file_paths = [
        r"\\smb2.fxnas02.pge.com\techopsautomation-fs01\metadata\fxnas02_filesystems.csv",
        r"\\smb.fxnas03.pge.com\techopsautomation-fs02\metadata\fxnas03_filesystems.csv",
        r"\\smb2.rcnas02.pge.com\techopsautomation-fs03\metadata\rcnas02_filesystems.csv",
        r"\\smb.rcnas03.pge.com\techopsautomation-fs04\metadata\rcnas03_filesystems.csv"
    ]






# #  Desired  Metrics (Used while extracting data (stats))  run_vrops_extraction -> fetch_metrics
# ## (Virtual  Machine) Metrics
# vmware_metrics_names = [
#         'mem|consumed_average',
#         'cpu|usage_average',
#         'guestfilesystem|usage_total',
#         'guestfilesystem|capacity_total'
#     ]

# esxi_metrics_names = [
#     "summary|number_running_vcpus",
#     "cpu|usage_average",
#     "mem|granted_average",
#     "mem|host_usagePct",
#     "mem|reservedCapacityPct",
#     "mem|overhead_average",
#     "sys|uptime_latest"
#   ]




# # Desired  metrics + properties
# ## Virtual Machine (VMWARE) 
# vmware_properties_names = ['config|name',
#  'summary|runtime|powerState',
#  'summary|folder',
#  'summary|customTag:PGE-AppID|customTagValue',
#  'config|version',
#  'summary|guest|toolsVersion',
#  'summary|guest|fullName',
#  'summary|guest|ipAddress',
#  'net:4000|mac_address',
#  'config|hardware|memoryKB',
#  'mem|consumed_average',
#  'config|hardware|numCpu',
#  'config|hardware|numCoresPerSocket',
#  'cpu|usage_average',
#  'config|hardware|diskSpace',
#  'guestfilesystem|usage_total',
#  'guestfilesystem|capacity_total',
#  'summary|parentHost',
#  'summary|parentCluster',
#  'summary|parentVcenter',
#  'summary|datastore',
#  'summary|customTag:Last Dell PowerProtect Backup|customTagValue',
#  'summary|tagJson',
#  'summary|customTag:LastSuccessfulBackup-com.dellemc.avamar|customTagValue']

# ## ESXi Host (metrics + properties)
# esxi_properties_names = ['config|name',
#                 'Certificate Summary|ESXi Host Certificate|End Date',
#                 'summary|parentVcenter', 
#                 'summary|parentDatacenter', 
#                 'summary|parentCluster', 
#                 'net|mgmt_address', 
#                 'summary|version', 
#                 'hardware|vendorModel', 
#                 'hardware|serialNumberTag', 
#                 'runtime|maintenanceState', 
#                 'runtime|connectionState', 
#                 'config|diskSpace', 
#                 'cpu|numCpuSockets', 
#                 'hardware|cpuInfo|numCpuCores', 
#                 'summary|number_running_vcpus', 
#                 'cpu|usage_average', 
#                 'hardware|memorySize', 
#                 'mem|granted_average', 
#                 'mem|host_usagePct', 
#                 'mem|reservedCapacityPct',
#                 'mem|overhead_average', 
#                 'cpu|cpuModel', 
#                 'sys|uptime_latest']


# # create mapping dictionary for our attributes {our_attributes : sheet headings}(preserve order)
# ## VMWare
# vmware_column_mapping = {
#     "config|name": "VM Name",
#     "summary|runtime|powerState": "Power State",
#     "summary|folder": "VCenter Folder",
#     "summary|customTag:PGE-AppID|customTagValue": "App ID",
#     "config|version": "VM Hardware Version",
#     "summary|guest|toolsVersion": "VM Tools Version",
#     "summary|guest|fullName": "Operating System",
#     "summary|guest|ipAddress": "Guest IP Address",
#     "net:4000|mac_address": "MAC Address",
#     "config|hardware|memoryKB": "Memory",
#     "mem|consumed_average": "Memory Utilization",
#     "config|hardware|numCpu": "Virtual CPU",
#     "config|hardware|numCoresPerSocket": "Cores Per Socket",
#     "cpu|usage_average": "CPU Usage",
#     "config|hardware|diskSpace": "Total Disk Space",
#     "guestfilesystem|usage_total": "Disk Utlization (TB)",
#     "guestfilesystem|capacity_total": "Disk Capacity",
#     "summary|parentHost": "Current Host",
#     "summary|parentCluster": "Cluster",
#     "summary|parentVcenter": "vCenter",
#     "summary|datastore": "Datastore",
#     "summary|customTag:Last Dell PowerProtect Backup|customTagValue": "PPDM Backup",
#     "summary|tagJson": "Vsphere",
#     "summary|customTag:LastSuccessfulBackup-com.dellemc.avamar|customTagValue": "Last Successful Backup"
# }

# ## ESXi Hosts
# esxi_column_mapping = {
#     'config|name': "Name",
#     'Certificate Summary|ESXi Host Certificate|End Date': "Cert END",
#     'summary|parentVcenter': "vCenter",
#     'summary|parentDatacenter': "Datacenter",
#     'summary|parentCluster': "Cluster",
#     'net|mgmt_address': "Mgm IP",
#     'summary|version': "ESXi Version",  # Note: This seems mismatched; consider verifying
#     'hardware|vendorModel': "HW Model",          # Also seems mismatched
#     'hardware|serialNumberTag': "HW Serial no",
#     'runtime|maintenanceState': "Maintenance state",
#     'runtime|connectionState': "Connection state",
#     'config|diskSpace': "Datastore Disk Space",
#     'cpu|numCpuSockets': "Socket",
#     'hardware|cpuInfo|numCpuCores': "Cores",
#     'summary|number_running_vcpus': "vCPU Allocated",
#     'cpu|usage_average': "Host CPU Usage %",
#     'hardware|memorySize': "Host Memory | GB",
#     'mem|granted_average': "Memory Allocated | GB",
#     'mem|host_usagePct': "Host Mem Usage %",
#     'mem|reservedCapacityPct': "Memory Reserved %",
#     'mem|overhead_average': "Memory Overhead",
#     'cpu|cpuModel': "CPU Model",
#     'sys|uptime_latest': "System|Uptime (Day(s))"
# }

# vmware_create_table_query = """
#         IF OBJECT_ID('dbo.VMware', 'U') IS NOT NULL DROP TABLE dbo.VMware;

#         CREATE TABLE dbo.VMware (
#             [VM Name] NVARCHAR(255),
#             [Power State] NVARCHAR(50),
#             [VCenter Folder] NVARCHAR(255),
#             [App ID] NVARCHAR(255),
#             [VM Hardware Version] NVARCHAR(255),
#             [VM Tools Version] NVARCHAR(255),
#             [Operating System] NVARCHAR(255),
#             [Guest IP Address] NVARCHAR(255),
#             [MAC Address] NVARCHAR(255),
#             [Memory] FLOAT,
#             [Memory Utilization] FLOAT,
#             [Virtual CPU] NVARCHAR(255),
#             [Cores Per Socket] NVARCHAR(50),
#             [CPU Usage] FLOAT,
#             [Total Disk Space] FLOAT,
#             [Disk Utlization (TB)] FLOAT,
#             [Disk Capacity Remaining] FLOAT,
#             [Current Host] NVARCHAR(255),
#             [Cluster] NVARCHAR(255),
#             [vCenter] NVARCHAR(255),
#             [Datastore] NVARCHAR(255),
#             [PPDM Backup] NVARCHAR(500),
#             [Last Successful Backup] NVARCHAR(255),
#             [Vsphere Tags] NVARCHAR(500)
#         )
#         """

# esxi_create_table_query = """
#     IF OBJECT_ID('dbo.ESXi', 'U') IS NOT NULL DROP TABLE dbo.ESXi;
    
#     CREATE TABLE dbo.ESXi (
#         [Name] NVARCHAR(255),
#         [Cert END] NVARCHAR(255),
#         [vCenter] NVARCHAR(255),
#         [Datacenter] NVARCHAR(255),
#         [Cluster] NVARCHAR(255),
#         [Mgm IP] NVARCHAR(255),
#         [ESXi Version] NVARCHAR(255),
#         [HW Model] NVARCHAR(255),
#         [HW Serial no] NVARCHAR(255),
#         [Maintenance state] NVARCHAR(255),
#         [Connection state] NVARCHAR(255),
#         [Datastore Disk Space] FLOAT,
#         [Socket] NVARCHAR(255),
#         [Cores] NVARCHAR(255),
#         [vCPU Allocated] FLOAT,
#         [Host CPU Usage %] FLOAT,
#         [Host Memory | GB] FLOAT,
#         [Memory Allocated | GB] FLOAT,
#         [Host Mem Usage %] FLOAT,
#         [Memory Reserved %] FLOAT,
#         [Memory Overhead] FLOAT,
#         [CPU Model] NVARCHAR(255),
#         [System|Uptime (Day(s))] FLOAT
#     );
#     """

# vmware_insert_sql_query = """
#         INSERT INTO dbo.VMware VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#         """

# esxi_insert_sql_query = """
#     INSERT INTO dbo.ESXi VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#     """
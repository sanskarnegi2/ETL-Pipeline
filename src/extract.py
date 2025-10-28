import os
import requests
import logging
import asyncio
import aiohttp
import time
import xmltodict
from pandas import json_normalize
from dotenv import load_dotenv
import warnings
import urllib3
from requests.exceptions import RequestException
from urllib3.exceptions import MaxRetryError

# Suppress only InsecureRequestWarning
warnings.simplefilter('ignore', urllib3.exceptions.InsecureRequestWarning)



# logging setup
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# get vrops identifiers
def get_vrops_identifiers(token, vrops_host, resourceKind='VirtualMachine'):

    # --- Headers ---
    headers = {
        'Content-Type': 'application/json',
        'Authorization': token,  
        'Accept': 'application/json'
    }
    
    # Pagination
    page = 0
    page_size = 1000
    all_resources = []

    try:
        while True:
            url = f'{vrops_host}/suite-api/api/resources?adapterKind=VMWARE&page={page}&pageSize={page_size}&resourceKind={resourceKind}&_no_links=true'
            try:
                # --- Make GET Request ---
                response = requests.get(url, headers=headers, verify=False)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f'Error fetching page {page}: {e}')

            # --- Parse Response ---
            data = response.json().get('resourceList', [])
            # data = response.json()

            if not data:
                    logger.info(f"All pages fetched {resourceKind} ids. Total resources: {len(all_resources)}")
                    # get all the identifiers
                    identifiers = [res['identifier'] for res in all_resources if 'identifier' in res]
                    return identifiers
                    # return statment will terminate the loop as it will terminate whole function
            
            all_resources.extend(data)
            logger.info(f"Fetched page {page} with {len(data)} resources for {resourceKind} ids")

            # increment page
            page += 1
            # if page == 1: # for testing only
            #         logger.info(f"All pages fetched {resourceKind} ids. Total resources: {len(all_resources)}")
            #         # get all the identifiers
            #         identifiers = [res['identifier'] for res in all_resources if 'identifier' in res]
            #         return identifiers
            #         # return statment will terminate the loop as it will terminate whole function



    except Exception as exc:
         logger.info(f'Something went wrong while getting vrops identifiers: {exc}')
    

# Fetch Metrics and properties for the
async def run_vrops_extraction(token, identifiers, vrops_host, desired_metrics, max_concurrent=40, resourceKind='VirtualMachine'):
    start_time = time.time()

    headers = {
        'Content-Type': 'application/json',
        'Authorization': token,
        'Accept': 'application/json'
    }


    ### Example desired_metrics
    # desired_metrics = [
    #     'mem|consumed_average',
    #     'cpu|usage_average',
    #     'guestfilesystem|usage_total',
    #     'guestfilesystem|capacity_total'
    # ]

    async def fetch_metrics(session, vm_id):
        url = f'{vrops_host}/suite-api/api/resources/{vm_id}/stats/latest?_no_links=true'
        try:
            async with session.get(url, headers=headers, ssl=False) as response:
                response.raise_for_status()
                metrics = await response.json()
                des_metrics = [
                    {'name': st['statKey']['key'], 'value': st['data'][0]}
                    for st in (metrics.get('values', [{}])[0].get('stat-list', {}).get('stat', []))
                    if st['statKey']['key'] in desired_metrics   # desired_metrics -> list of metrics 
                ]
                return des_metrics
        except Exception as e:
            logger.error(f"Metrics fetch failed for {vm_id}: {e}")
            return []

    async def fetch_properties(session, vm_id):
        url = f'{vrops_host}/suite-api/api/resources/{vm_id}/properties?_no_links=true'
        try:
            async with session.get(url, headers=headers, ssl=False) as response:
                response.raise_for_status()
                properties = (await response.json()).get('property', [])
                return properties
        except Exception as e:
            logger.error(f"Properties fetch failed for {vm_id}: {e}")
            return None

    async def fetch_vm_data(session, vm_id):
        metrics_task = fetch_metrics(session, vm_id)
        properties_task = fetch_properties(session, vm_id)
        metrics, properties = await asyncio.gather(metrics_task, properties_task)
        return {
            'vm_id': vm_id,
            'data': (properties or []) + (metrics or [])
        }

    async def main():
        connector = aiohttp.TCPConnector(limit=max_concurrent)
        async with aiohttp.ClientSession(connector=connector) as session:
            logger.info(f'Fetching Metrics and properties for {resourceKind}')
            tasks = [fetch_vm_data(session, vm_id) for vm_id in identifiers]
            results = await asyncio.gather(*tasks)
            return [r for r in results if r is not None]

    results = await main()
    elapsed = time.time() - start_time
    logger.info(f"Elapsed time for fetching {resourceKind} : {elapsed:.2f} seconds")
    return results

    # # Convert each VM's property list into dictionary
    # # Flatten the data
    # flattened_data = []
    # # for vm_props in res:
    # for vm_props in results:
    #     # if item['data'][0]['name'] in metric_names:
    #     vm_dict = {item['name']: item['value'] for item in vm_props['data'] if item['name'] in metric_names} 
    #     flattened_data.append(vm_dict)


# Get view names for AMPs  (not going to use in script, just for future recomdation in any confusion)
def get_amps_view_names(token):
    # base_url & route
    base_url = 'https://amps.cloud.pge.com/axe-platform'
    route = 'api/data-lake/v1/meta/dataviews?skip=0&take=0&properties=true&sourceList=true'

    # Header
    headers = {
                "Authorization": token,
                "Content-Type": "application/json"
            }
    
    # Construct Url
    url = f"{base_url}/{route}"

    # Make request
    response = requests.get(url, headers=headers, verify=False)
    
    if response.status_code == 200:
        # --- Parse Response ---
        data = response.json().get('data', [])
        view_list = [i['viewName'] for i in data]
        return view_list

    else:
        print(f"Error: {response.status_code} - {response.text}")

# Fetch AMPs Data
def fetch_amps_data(token, view_type, skip=0, take=2000):
    try:
        skip = 0
        take = 2000
        all_data = []
        base_url = 'https://amps.cloud.pge.com/axe-platform'
        
        # fetch data using pagination (skip, take)
        while True:
            # route & header
            route = f'api/data-lake/v1/dataview/{view_type}?skip={skip}&take={take}'
            headers = {
                "Authorization": token,
                "Content-Type": "application/json"
                # "Accept": "application/json"
            }
            
            # Construct url
            url = f"{base_url}/{route}"
            
            try:
                # --- Make GET Request ---
                response = requests.post(url, headers=headers, verify=False)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f'Error fetching data for {view_type}: {e}')
                
            
            # --- Parse Response ---
            data = response.json().get('data', [])
            # data = response.json()
            if not data:
                    logger.info(f"All data fetched for {view_type}. Total data: {len(all_data)}")
                    # return all data
                    return all_data
            
            all_data.extend(data)
            logger.info(f"Fetched {view_type} data {skip} - {skip + len(data)}")
            
            # increment skip -> skip += 2000
            skip += take
    except Exception as exception:
        logger.info("Something went wrong")

# Fetch DPA Data
## get node_ids
def get_node_id(token, session, query_value):
    # create a list to store node ids
    node_ids = []
    url = f"https://delldpa.utility.pge.com/apollo-api/nodes/?query=name={query_value}"
    headers = {
        "Content-Type": "application/vnd.emc.apollo-v1+xml",
        "Authorization": token
    }
    
    try:
        response = session.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            logger.info(f"Successfully retrieved node_ids for {query_value}")
            data_dict = xmltodict.parse(response.text)
            nodes =  data_dict['nodes']['node']
            if type(nodes) == list:
                for node in nodes:
                    node_id = node['id']
                    node_ids.append(node_id)
                    
            else:
                node_id = nodes['id']
                node_ids.append(node_id)
            return [{'query_value':query_value,'node_ids':node_ids}]
        else:
            logger.info(f"Request failed with status code: {response.status_code}")
            logger.info(f"Response content: {response.text}")
    except MaxRetryError:
        logger.info("Max retries exceeded. Server returned too many 500 errors: get node_id")
    except RequestException as e:
        logger.info(f"Request failed - get node_id: {str(e)}")    
    except Exception as e:
        logger.info(f"Error occurred while fetching node ID for {query_value}: {e}")
        # logger.exception(f"Error occurred while fetching node ID for {query_value}: {e}")
    
    return None

## get report url
def get_report_url(token, session, node_ids):
    # create a list to store report urls
    report_urls = []
    url = "https://delldpa.utility.pge.com/dpa-api/report"
    headers = {
        "Content-Type": "application/vnd.emc.apollo-v1+xml",
        "Authorization": token
    }
    
    for ids in node_ids:
        for node_id in ids['node_ids']:
            # Properly formatted XML payload
            xml_body = f"""
            <runReportParameters>
                <report>
                    <name>Backup All Jobs</name>
                </report>
                <nodes>
                    <node>
                        <id>{node_id}</id>
                    </node>
                </nodes>
                <timeConstraints type="window">
                    <window>
                        <name>Last Day</name>
                    </window>
                </timeConstraints>
                <formatParameters>
                    <formatType>CSV</formatType>
                </formatParameters>
            </runReportParameters>
            """
            
            try:
                response = session.post(url, headers=headers, data=xml_body, verify=False)
                if response.status_code == 201:
                    data_dict = xmltodict.parse(response.text)
                    report_url = data_dict['report']['link']
                    report_urls.append({'query_value':ids['query_value'],'report_url':report_url})
                else:
                    logger.info(f"Request failed with status code {response.status_code}")
                    logger.debug(response.text)
            
            except MaxRetryError:
                logger.info("Max retries exceeded. Server returned too many 500 errors: get report url")
            except RequestException as e:
                logger.info(f"Request failed - get report url: {str(e)}")
            except Exception as e:
                logger.info(f"Error during report_url request: {e}")
    
    return report_urls

# get dpa report
def get_dpa_report(token, session, report_urls):
    # create a list to store xml reports
    xml_reports = []
    logger.info("Inside get_dpa_report")

    headers = {
        "Content-Type": "application/vnd.emc.apollo-v1+xml",
        "Authorization": token
    }
    for report_url in report_urls:
        try:
            response = session.get(report_url['report_url'], headers=headers, verify=False)
            if response.status_code == 200:
                logger.info("Successfully retrieved the csv report.")
                # append the report into list
                xml_reports.append(response.text)
                time.sleep(2)
            else:
                logger.info(f"Request failed with status code: {response.status_code}")
                logger.debug(response.text)
        
        except MaxRetryError:
            logger.info("Max retries exceeded. Server returned too many 500 errors.")
        except RequestException as e:
            logger.info(f"Request failed: {str(e)}")
    
        except Exception as e:
            logger.info(f"Error occurred while fetching report: {e}")
            # logger.exception(f"Error occurred while fetching report: {e}")
    
    return xml_reports


import os
import numpy as np
import requests
import logging
import json
import base64
from pandas import json_normalize
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# logging setup
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



# get vrops tokem
def get_vrops_auth_token(username, password, auth_url):
    # Header & Payload
    headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
    }

    payload = {
        "username": username,
        "password": password,
    }

    try:
        # Make the POST request to acquire token
        response = requests.post(auth_url, headers=headers, data=json.dumps(payload), verify=False)
        response.raise_for_status()
        # Extract token from response 
        token = response.json().get('token')

        if token:
            logging.info(f"Token acquired: {token}")
            return f'OpsToken  {token}'
        else:
            logging.error("No token Recieved.")
            raise Exception("Authentication failed. No token received.")

    except requests.exceptions.RequestException as req_exc:
        logging.error(f"Login failed: {req_exc}")
        return None
    except Exception as exception:
        logging.error(f'Unabel to fetch the token: {exception}')


# Get AMPs Auth-tokem
def get_amps_auth_token(username, password, login_url, portal_url):
    # Headers & Payload
    payload = {
                "username": username,
                "password": password,
                "queryParams": {"showLoginForm": "true"},
                "url": portal_url
            }

    headers = {
                "Content-Type": "application/json; charset=utf-8"
            }

    try:
        # Make the POST request to acquire token
        response = requests.post(login_url, json=payload, headers=headers, verify=False)  # verify = False for staging only not recommended for prod
        response.raise_for_status()
        # Extract token from response
        token = response.json().get("token")

        if token:
            logging.info("AMPS login successful. Token acquired.")
            return f"Bearer {token}"
        else:
            logging.error("No token Recieved.")
            raise Exception("Authentication failed. No token received.")

    except requests.exceptions.RequestException as req_exc:
        logging.error(f"Login failed: {req_exc}")
        return None
    except Exception as exception:
        logging.error(f'Unabel to fetch the token: {exception}')


# Get AIOPS Auth-tokem to fetch SAN storage data
def get_aiops_auth_token(client_id, client_secret, auth_url):
    # Header and data
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id, 
        "client_secret": client_secret
    }


    try:
        # Make the POST request to acquire token
        response =  requests.post(auth_url, headers=headers, data=data, verify=False)  # verify = False for staging only not recommended for prod
        response.raise_for_status()
        # Extract token from response
        token = response.json().get("access_token")

        if token:
            logging.info("AIOPS login successful. Token acquired.")
            return f"Bearer {token}"
        else:
            logging.error("No token Recieved.")
            raise Exception("Authentication failed. No token received.")

    except requests.exceptions.RequestException as req_exc:
        logging.error(f"Login failed: {req_exc}")
        return None
    except Exception as exception:
        logging.error(f'Unabel to fetch the token: {exception}')


# Get DELL Auth-tokem to fetch SAN storage data
def get_ibm_auth_token(api_key, auth_url):
    # Header
    headers = {
        "accept": "application/json",
        "x-api-key": api_key
    }

    try:
        # Make the POST request to acquire token
        response =  requests.post(auth_url, headers=headers, verify=False)  # verify = False for staging only not recommended for prod
        response.raise_for_status()
        # Extract token from response
        token = response.json().get('result').get('token')

        if token:
            logging.info("IBM login successful. Token acquired.")
            return f"{token}"
        else:
            logging.error("No token Recieved.")
            raise Exception("Authentication failed. No token received.")

    except requests.exceptions.RequestException as req_exc:
        logging.error(f"Login failed: {req_exc}")
        return None
    except Exception as exception:
        logging.error(f'Unabel to fetch the token: {exception}')



# Convert columns with data list type into json
def convert_lists_to_json(df):    
    # detect columns that contains lists or dicts
    list_like_cols = [
        col for col in df.columns
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any()
    ]
    
    # Iterate through columns
    for col in list_like_cols:
        df[col] = df[col].apply(lambda x: str(x))

    logger.info(f"Detected List columns and converted into Json: {list_like_cols}")
    # return
    return df
    
# Get DPA token
def get_dpa_token(username, password):
    # Original string
    original_string = f"{username}:{password}"

    # Convert string to bytes
    string_bytes = original_string.encode('utf-8')

    # Encode to Base64
    base64_bytes = base64.b64encode(string_bytes)

    # Convert bytes back to string
    base64_string = base64_bytes.decode('utf-8')

    logger.info("Token generated for dpa")

    return f'Basic {base64_string}'

# create session to Request endpoints URLs
def create_session_with_retries():
    # Retry strategy
    retry_strategy = Retry(
        total=5,
        backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# drop duplicate columns
def remove_duplicate_cols(df):
    # get original column names
    org_cols = df.columns
    # set column names in lower case
    cols = [col.lower()for col in org_cols]
    df.columns = cols
    # get duplicate columns
    duplicates = df.columns[df.columns.duplicated()]
    print("Duplicate columns:", duplicates)
    drop_indices = []
    for dup_col in duplicates:
        indices = np.where(df.columns == dup_col)[0]
        # drop every duplicate columns except the original(first) one [1:] 
        drop_indices.extend(indices[1:])
    print(f'indices to drop: {drop_indices}')
    # set the original column names
    df.columns = org_cols
    # drop duplicate columns
    df.drop(columns = [org_cols[drop_indice] for drop_indice in drop_indices], inplace=True)

# convert the data(bytes) into TB for data stoage columns
def convert_into_tb(value, unit='T'):

    units = {
        'b': 1,               # bytes
        'k': 1024,                # kilobytes to bytes
        'M': 1024 ** 2,           # megabytes to bytes
        'G': 1024 ** 3,           # gigabytes to bytes
        'T': 1024 ** 4,           # terabytes to bytes
        'P': 1024 ** 5            # petabytes to bytes
        }

    # convert into bytes
    bytes_value = value * units[unit]
    # Convert bytes to TB
    tb_value = bytes_value / (1024 ** 4)
    return round(tb_value,2)
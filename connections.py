import logging
import sqlalchemy
import requests
import yaml
from requests.auth import HTTPBasicAuth
import paramiko

# Establish Logger
logger = logging.getLogger("cayuse_data_load.log")

# Read config file for credentials
with open('config.yaml', 'r', encoding='UTF-8') as file:
    config = yaml.safe_load(file)

# Currently using dev urls, need to be updated to production when ready
# Establish API constants
API_USER = config['api']['username']
API_PASSWORD = config['api']['password']
AUTH_URL = "https://signin.uat.cayuse.com"
BASE_URL = "https://hpu-t.uat.cayuse.com"

# Establish SFTP constants
SFTP_USERNAME = config["sftp"]["username"]
SFTP_PASSWORD = config["sftp"]["password"]
SFTP_HOST = config["sftp"]["host"]
SFTP_PORT = config["sftp"]["port"]
SFTP_KEY = config["sftp"]["ssh_key"]


def connect_to_db():
    # Establish db Credentials
    driver = config['db']['driver']
    username = config['db']['username']
    password = config['db']['password']
    server = config['db']['host']
    database = config['db']['database']

    # Create db engine
    logger.info("connecting to db")
    try:
        engine = sqlalchemy.create_engine(
            fr"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}")
        logger.info("connection successful")
        return engine
    except sqlalchemy.exc.InterfaceError as e:
        logger.error(e.args[1])
        raise SystemExit(e) from e
    except ConnectionError as e:
        logger.error(e.args[1])
        raise SystemExit(e) from e
    except Exception as e:
        raise SystemExit(e) from e


def get_token():
    url = AUTH_URL + "/basicauth"
    tenant_id = config["api"]["tenant_id"]
    header = {
        "username": API_USER,
        "password": API_PASSWORD
    }
    params = {
        "tenant_id": tenant_id
    }
    try:
        logger.info("requesting token...")
        send_request = requests.get(url=url,
                                    auth=HTTPBasicAuth(username=API_USER,
                                                       password=API_PASSWORD,
                                                       ),
                                    headers=header,
                                    params=params,
                                    timeout=50)
        response = send_request.text
        logger.info("token request successful")
        return response
    except requests.exceptions.Timeout as e:
        logging.info("error: %s", e)
        raise SystemExit(e) from e
    except requests.exceptions.ConnectionError as e:
        logging.info("error: %s", e)
        raise SystemExit(e) from e
    except requests.exceptions.JSONDecodeError as e:
        logging.info("error: %s", e)
        raise SystemExit(e) from e


def post_persons(csv, token):
    url = BASE_URL + "/api/v2/administration/batch/upload/user"
    header = {
        "username": API_USER,
        "password": API_PASSWORD,
        "Authorization": f"Bearer {token}"
    }
    params = {
        "send_account_activation_emails": "false"
    }
    try:
        logger.info("sending data file request...")
        send_request = requests.post(url=url,
                                     headers=header,
                                     files={"file": csv},
                                     params=params,
                                     timeout=50)
        response = send_request.json()
        print(response)
        return response
    except requests.exceptions.Timeout as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e
    except requests.exceptions.ConnectionError as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e


def post_units(csv, token):
    url = BASE_URL + "/api/v2/administration/batch/upload/unit"
    header = {
        "username": API_USER,
        "password": API_PASSWORD,
        "Authorization": f"Bearer {token}"
    }
    params = {
        "send_account_activation_emails": "false"
    }
    print(url)
    try:
        logger.info("sending data file request...")
        send_request = requests.post(url=url,
                                     headers=header,
                                     files={"file": csv},
                                     params=params,
                                     timeout=50)
        response = send_request.json()
        if send_request.status_code == 200:
            return response
        else:
            logger.warning("api call returned no data")
    except requests.exceptions.Timeout as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e
    except requests.exceptions.ConnectionError as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e


def post_internal_associations(csv, token):
    url = BASE_URL + "/api/v2/administration/batch/upload/affiliation"
    header = {
        "Authorization": f"Bearer {token}",
        "X-Idp-New-Login": "true",
        "Content-Type": "text/csv"
    }
    # params = {
    #
    # }
    print(url)
    try:
        logger.info("sending data file request...")
        send_request = requests.post(url=url,
                                     headers=header,
                                     files={"file": csv},
                                     timeout=50)
        response = send_request.json()
        print(response)
        return response
    except requests.exceptions.Timeout as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e
    except requests.exceptions.ConnectionError as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e


def check_status(job_id, token, endpoint):
    url = BASE_URL + f"/api/v2/administration/batch/status/{endpoint}"
    header = {
        "Authorization": f"Bearer {token}",
        "X-Idp-New-Login": "true"
    }
    params = {
        "jobId": job_id
    }
    print(url)
    try:
        logger.info("sending data file request...")
        send_request = requests.get(url=url,
                                    headers=header,
                                    params=params,
                                    timeout=50
                                    )
        response = send_request.json()
        return response
    except requests.exceptions.Timeout as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e
    except requests.exceptions.ConnectionError as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e


def get_job_report(job_id, token, endpoint):
    url = BASE_URL + f"/api/v2/administration/batch/report/{endpoint}"
    header = {
        "Authorization": f"Bearer {token}",
        "X-Idp-New-Login": "true"
    }
    params = {
        "jobId": job_id
    }
    print(url)
    try:
        logger.info("sending data file request...")
        send_request = requests.get(url=url,
                                    headers=header,
                                    params=params,
                                    timeout=50
                                    )
        response = send_request.text
        return response
    except requests.exceptions.Timeout as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e
    except requests.exceptions.ConnectionError as e:
        logging.info(e)
        print(e)
        raise SystemExit(e) from e


def connect_to_sftp():
    logger.info("establishing sftp client")
    # Load SFTP client and key
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        # Make connection to sftp server and return connection
        client.connect(hostname=SFTP_HOST, port=SFTP_PORT,
                       username=SFTP_USERNAME, key_filename=SFTP_KEY)
        logger.info("connection successful")
        sftp_session = client.open_sftp()
        return sftp_session
    except Exception as e:
        logger.warning("connection failed: %s", e)
        raise SystemExit(e) from e

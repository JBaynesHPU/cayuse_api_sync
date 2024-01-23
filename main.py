import logging
import os
import shutil
import time
import datetime as dt

# import pandas as pd
from connections import (
    post_persons,
    get_token,
    post_units,
    post_internal_associations,
    check_status,
    connect_to_sftp,
    get_job_report,
)
import bulk_load

# import transaction_import
import personnel_data_load

# from sqlalchemy import MetaData, Table, select
# from connections import connect_to_db as db

logging.basicConfig(
    filename="cayuse_data_load.log",
    filemode="a",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cayuse_data_load.log")
logger.setLevel(logging.INFO)
logger.info("starting script...")


def create_report(report_data):
    pass


def send_file_sftp():
    cnx = connect_to_sftp()
    src_loc = "output/fund_manager"
    for file in os.listdir(src_loc):
        file_path = os.path.join(src_loc, file)
        target_path = rf"{file}"
        try:
            logger.info("sending %s", file)
            cnx.put(
                localpath=file_path, remotepath=target_path, callback=None, confirm=True
            )
            logger.info("file uploaded successfully")

            # Save File for historical record
            date = dt.date.today().strftime("%Y.%m.%d")
            sent_folder = rf"C:\Users\jbaynes\dev\notebooks\cayuse_api_sync\sftp_archive\{file}_{date}.csv"
            shutil.copy(file_path, sent_folder)
        except IOError as e:
            print(e)


if __name__ == "__main__":
    bulk_load.main()
    personnel_data_load.main()
    # transaction_import.main()

    # Connect to API and generate token for authentication
    token = get_token()
    print(token)

    with open(
        r"output\hr_connect\Bulk_Data_Load.csv", encoding="UTF-8"
    ) as admin_bulk_load:
        job_id = post_persons(csv=admin_bulk_load, token=token)["jobId"]
        admin_bulk_load.close()
    processing = True
    while processing:
        status = check_status(job_id=job_id, token=token, endpoint="user")
        if status["status"] == "Completed":
            print(status)
            if int(status["errors"]) > 0:
                report = get_job_report(
                    job_id=job_id, token=token, endpoint="user")
                create_report(report)
            processing = False
        else:
            time.sleep(15)

    with open(r"output\hr_connect\OrgUnits.csv", encoding="UTF-8") as org_units:
        org_units_job_id = post_units(csv=org_units, token=token)["jobId"]
        org_units.close()
    processing = True
    while processing:
        status = check_status(job_id=org_units_job_id,
                              token=token, endpoint="unit")
        if status["status"] == "Completed":
            if int(status["errors"]) > 0:
                report = get_job_report(
                    job_id=org_units_job_id, token=token, endpoint="unit"
                )
                create_report(report)
            processing = False
        else:
            time.sleep(15)

    with open(
        r"output\hr_connect\InternalAssociations.csv", encoding="UTF-8"
    ) as internal_associations:
        associations_job_id = post_internal_associations(
            csv=internal_associations, token=token
        )["jobId"]
        internal_associations.close()
    processing = True
    while processing:
        status = check_status(
            job_id=associations_job_id, token=token, endpoint="affiliation"
        )
        if status["status"] == "Completed":
            if int(status["errors"]) > 0:
                report = get_job_report(
                    job_id=associations_job_id, token=token, endpoint="affiliation"
                )
                print(report)
                create_report(report)
            processing = False
        else:
            time.sleep(15)
    # Send transactional data to Fund Manager SFTP
    # send_file_sftp()

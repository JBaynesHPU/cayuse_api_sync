import logging
import pandas as pd
from sqlalchemy import MetaData, Table, select
from connections import connect_to_db as db

logger = logging.getLogger("cayuse_data_load.log")


def clean_id(user_id):
    if len(str(user_id)) < 7:
        user_id = str(user_id).zfill(7)
        return user_id
    else:
        return user_id


def check_names(df):
    # Establish dataset and tables needed to cross-reference names and emails
    user_list = df
    engine = db()
    metadata = MetaData()
    employees_table = Table("CACHED_IDM_EMPLOYEES", metadata,
                            autoload_with=engine.connect())
    students_table = Table("CACHED_IDM_STUDENTS", metadata,
                            autoload_with=engine.connect())

    # Iterate through dataset, checking for changes in first/last name and email address.
    # Update if necessary and save changes to data set. * Returns the dataframe that will be sent in the data file *
    try:
        for index, row in user_list.iterrows():
            emp_id = int(row["Employee ID"])
            padded_id = clean_id(emp_id)
            row["Employee ID"] = padded_id
            query = select(employees_table).where(
                employees_table.c.ID == int(padded_id))
            with engine.connect() as connection:
                result = connection.execute(query).fetchall()
            if len(result) > 0:
                first_name = row["First Name"]
                last_name = row["Last Name"]
                email = row["Email"]
                if first_name == result[0][2]:
                    pass
                else:
                    row["First Name"] = result[0][2]
                if last_name == result[0][3]:
                    pass
                else:
                    row["Last Name"] = result[0][3]
                if email == result[0][8]:
                    pass
                else:
                    row["Email"] = result[0][8]
                user_list.loc[index] = row
            else:
                query = select(students_table).where(
                    students_table.c.ID == int(padded_id))
                with engine.connect() as connection:
                    students_result = connection.execute(query).fetchall()
                if len(students_result) > 0:
                    first_name = row["First Name"]
                    last_name = row["Last Name"]
                    email = row["Email"]
                    if first_name == students_result[0][2]:
                        pass
                    else:
                        row["First Name"] = students_result[0][2]
                    if last_name == students_result[0][3]:
                        pass
                    else:
                        row["Last Name"] = students_result[0][3]
                    if email == students_result[0][8]:
                        pass
                    else:
                        row["Email"] = students_result[0][8]

                    row["Employee ID"] = padded_id
                    user_list.loc[index] = row
                else:
                    # with open("output/check_names.txt", mode='a') as check_file:
                    #     check_file.write(str(padded_id) + '\n')
                    pass

        user_list["Employee ID"] = user_list["Employee ID"].astype(str)
        return user_list
    except KeyError as error:
        if error == "First Name" or "Last Name" or "Email":
            pass
        else:
            print(error)
            raise SystemExit(error) from error


def clean_internal_associations(df):
    for index, row in df.iterrows():
        emp_id = int(row["Employee ID"])
        padded_id = clean_id(emp_id)
        row["Employee ID"] = padded_id
        # Update Primary Code
        prefix = "CC"
        row["Unit Primary Code"] = prefix + str(row["Unit Primary Code"])
        # Save changes to row
        df.loc[index] = row

    return df


def clean_org_units(df):
    for index, row in df.iterrows():
        # Update Primary Code
        primary_code = str(row["Primary Code"])
        parent_unit_code = str(row["Parent Unit Primary Code"])[:-2]
        if primary_code == "0":
            primary_code = "101010"
            # parent_unit_code = np.NaN
        if parent_unit_code == "0":
            parent_unit_code = "101010"
        elif parent_unit_code == "n":
            parent_unit_code = "101010"
        row["Primary Code"] = int(primary_code)
        row["Parent Unit Primary Code"] = int(parent_unit_code)
        # Save changes to row
        df.loc[index] = row

    return df


def load_people():
    try:
        people = pd.read_excel(r"C:\Users\jbaynes\Documents\Documentation\FundManager\HPU_Admin_Bulk_Load.xlsx", "People",
                               index_col=False)
        return pd.DataFrame(people)
    except IOError as e:
        logging.error(e)


def load_org_units():
    try:
        org_units = pd.read_excel(r"C:\Users\jbaynes\Documents\Documentation\FundManager\HPU_Admin_Bulk_Load.xlsx", "Org Units",
                                  index_col=False)
        return pd.DataFrame(org_units)
    except IOError as e:
        logging.error(e)


def load_internal_associations():
    try:
        internal_associations = pd.read_excel(r"C:\Users\jbaynes\Documents\Documentation\FundManager\HPU_Admin_Bulk_Load.xlsx", "Internal Associations",
                                              index_col=False)
        return pd.DataFrame(internal_associations)
    except IOError as e:
        logging.error(e)


def main():
    # Load excel tabs needed into a dataframe
    logger.info("loading bulk data excel file")
    bulk_load_data_people = load_people()
    org_units = load_org_units()
    internal_associations = load_internal_associations()

    # Check names and emails for updates
    logger.info("checking names and emails for updates...")
    clean_people = check_names(df=bulk_load_data_people)
    logger.info("names and emails updated")
    clean_ia = clean_internal_associations(df=internal_associations)
    # clean_orgs = clean_org_units(org_units)

    # Write DataFrames to csv to send to API
    clean_people.to_csv(r"output\hr_connect\Bulk_Data_Load.csv", index=False)
    org_units.to_csv(r"output\hr_connect\OrgUnits.csv", index=False)
    clean_ia.to_csv(r"output\hr_connect\InternalAssociations.csv", index=False)

    logger.info("ADMIN BULK LOAD FILES CREATED")

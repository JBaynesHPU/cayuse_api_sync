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


def check_titles(df):
    user_list = df
    engine = db()
    metadata = MetaData()
    employees_table = Table("CACHED_IDM_EMPLOYEES", metadata,
                               autoload_with=engine.connect())

    # Iterate through dataset, checking for changes in title name and department. Update if necessary and save
    # changes to data set. * Returns the dataframe that will be sent in the data file *
    for index, row in user_list.iterrows():
        emp_id = row["Personnel_Identifier"]
        if row["Personnel_Type_Code"] == "F" or row["Personnel_Type_Code"] == "S":
            print(f"{emp_id:07}")
            query = select(employees_table).where(
                employees_table.c.ID == int(f"{emp_id:07}"))
            result = engine.execute(query).fetchall()
            print(result)
            if len(result) > 0:
                title = row["Person_Title_Name"]
                # org_code = row["Organization_Unit_Code"]
                start_date = row["Start_Date"]
                if title == result[0][6]:
                    pass
                else:
                    row["Person_Title_Name"] = result[0][6]
                if start_date == result[0][-2]:
                    pass
                else:
                    row["Start_Date"] = result[0][-2]
            else:
                with open("output/check_titles.txt", mode="a", encoding='UTF-8') as file:
                    file.write(str(emp_id) + "\n")
                # pass

            user_list.loc[index] = row
        else:
            pass

    return user_list


def load_personnel_title():
    try:
        personnel_title = pd.read_excel(r"C:\Users\jbaynes\Documents\Documentation\FundManager\HPU_Personnel_Data_Load.xlsx", "Personnel_Title.csv",
                                        index_col=False)
        return pd.DataFrame(personnel_title)
    except IOError as e:
        logging.error(e)


def load_salary():
    try:
        salary = pd.read_excel(r"C:\Users\jbaynes\Documents\Documentation\FundManager\HPU_Personnel_Data_Load.xlsx", "Salary.csv",
                               index_col=False)
        return pd.DataFrame(salary)
    except IOError as e:
        logging.error(e)


def load_salary_dist():
    try:
        salary_dist = pd.read_excel(
            r"C:\Users\jbaynes\Documents\Documentation\FundManager\HPU_Personnel_Data_Load.xlsx", "Salary_Distribution.csv",
            index_col=False)
        return pd.DataFrame(salary_dist)
    except IOError as e:
        logging.error(e)


def load_pay_period():
    try:
        pay_period = pd.read_excel(r"C:\Users\jbaynes\Documents\Documentation\FundManager\HPU_Personnel_Data_Load.xlsx", "Pay_Period.csv",
                                   index_col=False)
        return pd.DataFrame(pay_period)
    except IOError as e:
        logging.error(e)


def main():
    # Load dataframes for processing
    pt_df = load_personnel_title()
    salary_df = load_salary()
    salary_dist = load_salary_dist()
    pay_period_df = load_pay_period()

    # Connect to db
    personnel_title = check_titles(pt_df)

    personnel_title.to_csv(r"output\hr_connect\Personnel_Title.csv")
    salary_df.to_csv(r"output\hr_connect\Salary.csv")
    salary_dist.to_csv(r"output\hr_connect\Salary_Distribution.csv")
    pay_period_df.to_csv(r"output\hr_connect\Pay_Period.csv")

    # with pd.ExcelWriter(r"output\hr_connect\personnel_data_load.xlsx",) as file:
    #     personnel_title.to_excel(file, sheet_name="Personnel_Title.csv", index=None)
    #     salary_df.to_excel(file, sheet_name="Salary.csv", index=None)
    #     salary_dist.to_excel(file, sheet_name="Salary_Distribution.csv", index=None)
    #     pay_period_df.to_excel(file, sheet_name="Pay_Period.csv", index=None)

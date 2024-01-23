import logging
import connections
import datetime as dt
from sqlalchemy import MetaData, Table, select
from pandas import DataFrame, pandas as pd
from connections import connect_to_db as db


def load_transactions():
    try:
        transactions = pd.read_excel(r"C:\Users\aboyles\Documents\Documentation\FundManager\HPU_Transaction_Import.xlsx",
                                     "Transactions",
                                     index_col=None)
        return transactions
    except pd.errors as e:
        logging.error(e)


def load_fund_codes():
    try:
        transactions = pd.read_excel(r"C:\Users\aboyles\Documents\Documentation\FundManager\Foundational_Data_Template.xlsx",
                                     "Fund_Codes",
                                     index_col=None)
        return transactions
    except pd.errors as e:
        logging.error(e)


def main():
    transactions_df = load_transactions()
    fund_codes = load_fund_codes()
    current_codes = fund_codes["Fund_Code"]

    print(transactions_df)
    print(current_codes)

    # with pd.ExcelWriter(r"output\transaction_import.xlsx", ) as file:
    #     transactions_df.to_excel(file, sheet_name="Personnel_Title.csv", index=None)



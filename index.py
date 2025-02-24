import pandas as pd
import sqlite3

db_connection = sqlite3.connect('data.db')
cursor = db_connection.cursor()


def extract_data():
    df_a = pd.read_csv("csv_files/order_region_a.csv")
    df_b = pd.read_csv("csv_files/order_region_b.csv")
    print(df_a.head())
    print(df_b.head())


def transform_data():
    pass
    
extract_data()
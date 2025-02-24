import pandas as pd
import sqlite3
import re


db_connection = sqlite3.connect('data.db')
cursor = db_connection.cursor()

def data_clean(df):
    # cleaning the column  PromotionDiscount and extracting the discount amount in a new column called discount_amount
    df['discount_amount']  = df['PromotionDiscount'].str.extract(r'"Amount"\s*:\s*"([\d\.]+)"')
    df['discount_amount']  =  df['discount_amount'].astype(float)
    # print(df.head())
    return df
    

def transform_data(df,region):
    df['total_sales'] = df['QuantityOrdered'] * df['ItemPrice']
    df['net_sale']   = df['total_sales'] - (df['QuantityOrdered'] *  df['discount_amount']) # 
    df['region'] = region
    df.drop('discount_amount', axis=1,inplace=True) # droping the column becuase no longer needed 
    # print(df.head())
    return df


def extract_data():
    df_a = pd.read_csv("csv_files/order_region_a.csv")
    df_b = pd.read_csv("csv_files/order_region_b.csv")
    
    df_a = data_clean(df_a) 
    df_a = transform_data(df_a,"A")
    df_b = data_clean(df_b)
    df_b = transform_data(df_b,"B")
    
    sales_data =  pd.concat([df_a, df_b], ignore_index=True)
    # print(sales_data)
    sales_data = sales_data.drop_duplicates(subset=['OrderId'], keep='first') # deleting duplicates and keeping the first occurence 
    sales_data = sales_data[sales_data['total_sales'] >= 0]
    # print(sales_data)
    load_into_db(sales_data)
    


def load_into_db(data):
    data.to_sql('sales_data', db_connection, if_exists='replace', index=False)
    db_connection.commit()


def read_data():
    
    queris = {"get_count":"SELECT COUNT(*) FROM sales_data",
            "total_sales_by_region":"SELECT region, SUM(total_sales) AS total_sales_amount FROM sales_data GROUP BY region",
            "average_sales_amount/transaction":"SELECT AVG(total_sales) AS average_sales_amount FROM sales_data",
            "find_duplicates_based_order_id":"SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1"
            }
    
    for key, query  in queris.items():
        cursor.execute(query)
        value = cursor.fetchall()
        print(value)        
    
    
    

extract_data()
read_data()
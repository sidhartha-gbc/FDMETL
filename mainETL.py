import mysql.connector
import pandas as pd
from dbUtility.SQLUtilities import *
import json
import pymysql
from sqlalchemy import create_engine


def managerTable(df : pd.DataFrame,engine,connection,tableName,primaryKey):
    df = df[['Region','Regional Manager']]
    df = df.drop_duplicates()
    df.columns = ['region','regional_manager']
        
    database_upsert(sqlalchamyEngine=engine,
                    mySQLconnectionObject=connection,
                    tableName=tableName,
                    pandasDataframe=df,
                    primaryKey=primaryKey
                    )
    print("Success")
    
def cityTable(df : pd.DataFrame,engine,connection,tableName,primaryKey):
    df = df[["Postal Code","City","State","Region","Country/Region"]]
    df = df.drop_duplicates()
    df.columns = ['postal_code','city','state','region','country']
        
    database_upsert(sqlalchamyEngine=engine,
                    mySQLconnectionObject=connection,
                    tableName=tableName,
                    pandasDataframe=df,
                    primaryKey=primaryKey
                    )
    print("Success")
    
def customerTable(df : pd.DataFrame,engine,connection,tableName,primaryKey):
    df = df[['Customer ID','Customer Name','Segment']]
    df = df.drop_duplicates()
    df.columns = ['customer_id','customer_name','segment']
        
    database_upsert(sqlalchamyEngine=engine,
                    mySQLconnectionObject=connection,
                    tableName=tableName,
                    pandasDataframe=df,
                    primaryKey=primaryKey
                    )
    print("Success")
    
def returnsTable(df : pd.DataFrame,engine,connection,tableName,primaryKey):
    df = df[['Order ID','Returned']]
    df = df.drop_duplicates()
    
    df.columns = ['order_id','is_returns']
    # df['is_returns'] = df['is_returns'].replace({"Yes"})
    # print(df)
    database_upsert(sqlalchamyEngine=engine,
                    mySQLconnectionObject=connection,
                    tableName=tableName,
                    pandasDataframe=df,
                    primaryKey=primaryKey
                    )
    print("Success")
    
def OrdersTable(df : pd.DataFrame,engine,connection,tableName,primaryKey):

    df = df[['Order ID','Ship Date','Ship Mode','Customer ID','Postal Code']]
    df = df.drop_duplicates()
    df.columns = ['order_id','ship_date','ship_mode','customer_id','postal_code']
        
    database_upsert(sqlalchamyEngine=engine,
                    mySQLconnectionObject=connection,
                    tableName=tableName,
                    pandasDataframe=df,
                    primaryKey=primaryKey
                    )
    print("Success")
    
def ProductTable(df : pd.DataFrame,engine,connection,tableName,primaryKey):
    df = df[['Product ID','Product Name','Category','Sub-Category']]
    df = df.drop_duplicates(subset=['Product ID'],keep='first')
    df.columns = ['product_id','product_name','category_name','subcategory_name']
    
    database_upsert(sqlalchamyEngine=engine,
                    mySQLconnectionObject=connection,
                    tableName=tableName,
                    pandasDataframe=df,
                    primaryKey=primaryKey
                    )
    print("Success")
    
def Order_ProductTable(df : pd.DataFrame,engine,connection,tableName,primaryKey):
    df['Discount'] =[round(x,2) for x in df['Discount']]
    df['Profit'] =[round(x,2) for x in df['Profit']]
    df['Sales'] =[round(x,2) for x in df['Sales']]
    df['order_product_id'] = df['Order ID'] + df['Product ID']
    df = df[['order_product_id','Order ID','Product ID','Quantity','Sales','Discount','Profit']]
    
    df = df.drop_duplicates(subset=['order_product_id'])
    df.columns = ['order_product_id','order_id','product_id','quantity','sales','discount','profit']
    
    database_upsert(sqlalchamyEngine=engine,
                    mySQLconnectionObject=connection,
                    tableName=tableName,
                    pandasDataframe=df,
                    primaryKey=primaryKey
                    )
    print("Success")
    

if __name__ == "__main__":
    # Reading the dataset
    
    peopleData = pd.read_excel("rawData/SampleSuperstoreV1_Excel2019.xlsx",sheet_name="People")
    mainTable = pd.read_excel("rawData/SampleSuperstoreV1_Excel2019.xlsx",sheet_name="Orders")
    Returns = pd.read_excel("rawData/SampleSuperstoreV1_Excel2019.xlsx",sheet_name="Returns")
    
    
    file = open("config/dev.json")
    config = json.load(file)
    
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    engine = create_engine("mysql+pymysql://" + config['user'] + ":" + config['password'] + "@" + config['host'] + "/" + config['database'],pool_pre_ping=True)
    
    
    
    managerTable(df=peopleData,
                 engine=engine,
                 connection=connection,
                 tableName="manager",
                 primaryKey="region")
    
    
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    cityTable(df=mainTable,
                 engine=engine,
                 connection=connection,
                 tableName="city",
                 primaryKey="postal_code")
    connection.close()
    
    
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    customerTable(df=mainTable,
                 engine=engine,
                 connection=connection,
                 tableName="customer",
                 primaryKey="customer_id")
    connection.close()
    
    
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    returnsTable(df=Returns,
                 engine=engine,
                 connection=connection,
                 tableName="returns",
                 primaryKey="is_returns")
    connection.close()
    
    
    
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    OrdersTable(df=mainTable,
                 engine=engine,
                 connection=connection,
                 tableName="orders",
                 primaryKey="order_id")
    connection.close()
    
    
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    ProductTable(df=mainTable,
                 engine=engine,
                 connection=connection,
                 tableName="products",
                 primaryKey="product_id")
    connection.close()
    
    
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    Order_ProductTable(df=mainTable,
                 engine=engine,
                 connection=connection,
                 tableName="order_product",
                 primaryKey="order_product_id")
    connection.close()
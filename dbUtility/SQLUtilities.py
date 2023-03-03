import mysql.connector
from mysql.connector import Error
import numpy as np

def create_server_connection(hostname,user_name,user_password):
    connection = None
    try:
        connection =  mysql.connector.connect(user=user_name, 
                                              password=user_password,
                                              host=hostname)
        print("MYSQL connected successfully")
        return connection
    except Exception as e:
        print("Unable to connect due to: ", e)

def run_queries(connection,query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        cursor.close()
        print("query Sucess")
    except Exception as e:
        print("Error Due to: ",e)
        
def return_queries(connection,query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        description = cursor.description
        data = cursor.fetchall()
        cursor.close()
        return data,description
        print("query Sucess")
    except Exception as e:
        print("Error Due to: ",e)
        
def database_connection(hostname,user_name,user_password,database):
    connection = None
    try:
        connection =  mysql.connector.connect(user=user_name, 
                                              password=user_password,
                                              host=hostname,
                                              database = database)
        print("MYSQL connected successfully")
        return connection
    except Exception as e:
        print("Unable to connect due to: ", e)
        
        
def database_upsert(sqlalchamyEngine,mySQLconnectionObject,tableName,pandasDataframe,primaryKey):
    
    try:
        ## remove primary key based rows in the table 
        keys = tuple(pandasDataframe[primaryKey].values)
        deleteRowQuerry = "DELETE FROM "+ tableName + "  WHERE " + primaryKey + " in {}".format(keys)
        changeConstarint = "SET FOREIGN_KEY_CHECKS=OFF;"
        run_queries(mySQLconnectionObject,changeConstarint)
        run_queries(mySQLconnectionObject,deleteRowQuerry)
        mySQLconnectionObject.close()
        print("pushing data in SQL Table: ",tableName )
        
        pandasDataframe.to_sql(name=tableName,con = sqlalchamyEngine,if_exists='append',index=False)
    except Exception as e:
        print("unable to Push data in table",tableName)
        print("'ERROR due to: ",e)
        
        
    
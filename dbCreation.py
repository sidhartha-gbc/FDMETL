
from dbUtility.SQLUtilities import create_server_connection,run_queries
import json


dbCreationQuery = "CREATE DATABASE "

if __name__ =="__main__":
    file = open("config/dev.json")
    config = json.load(file)
    connection = create_server_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'])
    
    dbCreationQuery = dbCreationQuery + config['database']
    run_queries(connection,dbCreationQuery)
    
    

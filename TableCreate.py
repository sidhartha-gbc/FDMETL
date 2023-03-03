"""
Run the query only after dbCreation.py has successfully executed
"""

import mysql.connector
from mysql.connector import Error
from dbUtility.SQLUtilities import create_server_connection,run_queries,database_connection
import json

manager = """CREATE TABLE Manager  (
    region VARCHAR(50) NOT NULL,
    regional_manager VARCHAR(100) NOT NULL,
    PRIMARY KEY (region));
"""

city = """CREATE TABLE City (
    postal_code VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    region VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    PRIMARY KEY (postal_code),
    FOREIGN KEY (region) REFERENCES Manager(region));
"""

customers = """CREATE TABLE Customer (
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    segment VARCHAR(100) NOT NULL,
    PRIMARY KEY (customer_id));
"""

returns = """CREATE TABLE Returns (
    order_id VARCHAR(50) NOT NULL,
    is_returns VARCHAR(50) NOT NULL,
    PRIMARY KEY (order_id));
"""




Order_Details = """CREATE TABLE Orders  (
  order_id VARCHAR(50) NOT NULL,
  ship_date VARCHAR(50) NOT NULL,
  ship_mode VARCHAR(50) NOT NULL,
  customer_id VARCHAR(50) NOT NULL,
  postal_code VARCHAR(50) NOT NULL,
  PRIMARY KEY (order_id),
  FOREIGN KEY (customer_id) REFERENCES Customer(customer_id),
  FOREIGN KEY (postal_code) REFERENCES City(postal_code));
"""

Product = """CREATE TABLE Products  (
  product_id VARCHAR(50) NOT NULL,
  product_name VARCHAR(500) NOT NULL,
  category_name VARCHAR(100) NOT NULL,
  subcategory_name VARCHAR(100) NOT NULL,
  PRIMARY KEY (product_id))
"""



Order_Product = """CREATE TABLE Order_Product (
  order_product_id VARCHAR(100) NOT NULL,
  order_id VARCHAR(50) NOT NULL,
  product_id VARCHAR(50) NOT NULL,
  quantity DECIMAL(10,5) NOT NULL,
  sales DECIMAL(10,5) NOT NULL,
  discount DECIMAL(10,5) NOT NULL,
  profit DECIMAL(10,5) NOT NULL,
  PRIMARY KEY (order_product_id),
  FOREIGN KEY (order_id) REFERENCES Orders(order_id),
  FOREIGN KEY (product_id) REFERENCES Products(product_id));
"""

Order_Product_delete = "DROP TABLE Orders,Order_Product;"


if __name__ == "__main__":
    
    file = open("config/dev.json")
    config = json.load(file)
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    run_queries(connection,Order_Product_delete)
    run_queries(connection,manager)
    run_queries(connection,city)
    run_queries(connection,customers)
    run_queries(connection,returns)
    run_queries(connection,Order_Details)
    run_queries(connection,Product)
    run_queries(connection,Order_Product)
        

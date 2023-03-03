

from dbUtility.SQLUtilities import database_connection,run_queries,return_queries
import json
import pandas as pd

data_pull_querry = """
    Select 
MANAGER_MERGED.*,
returns.is_returns
from (
select 
POSTAL_MERGED.*,
manager.regional_manager
  from( select 
CUSTOMER_MERGED.*,
city.city,
city.state,
city.region,
city.country from (
select 
PRODUCT_MERGED.*,
customer.customer_name,
customer.segment from(
select 
BASE.*,
products.product_name,
products.category_name,
products.subcategory_name from
(select 
	order_product.order_product_id,
    order_product.order_id,
    order_product.product_id,
    order_product.quantity,
    order_product.sales,
    order_product.discount,
    order_product.profit,
    orders.ship_date,
    orders.ship_mode,
    orders.customer_id,
    orders.postal_code
    from order_product LEFT JOIN orders on order_product.order_id = orders.order_id) AS BASE
    LEFT JOIN products ON BASE.product_id = products.product_id) AS PRODUCT_MERGED LEFT JOIN 
    customer on PRODUCT_MERGED.customer_id = customer.customer_id) AS CUSTOMER_MERGED LEFT JOIN 
    city ON CUSTOMER_MERGED.postal_code = city.postal_code) AS POSTAL_MERGED LEFT JOIN manager on 
    manager.region = POSTAL_MERGED.region) AS MANAGER_MERGED LEFT JOIN returns on 
    returns.order_id = MANAGER_MERGED.order_id  ;
    """
    
if __name__ =="__main__":
    file = open("config/dev.json")
    config = json.load(file)
    connection = database_connection(user_name=config['user'],
                                          user_password=config['password'],
                                          hostname=config['host'],
                                          database=config['database'])
    
    
    output,columnNames = return_queries(connection,data_pull_querry)
    
    fieldName = [i[0] for i in columnNames]
    data = pd.DataFrame(output)
    
    data.columns = fieldName
    
    
    # data pulled successfully from sql data base 
    
    print("starting to build operational report")
    data['ship_date'] = pd.to_datetime(data['ship_date'])
    data['quarter'] = data['ship_date'].dt.to_period('Q')
    
    data["year"] = [int(str(x).split("Q")[0]) for x in data.quarter]
    data["quarter"] = [str(x).split("Q")[1] for x in data.quarter]
    data["quarter"] = "Q" + data['quarter']
    
    data = data[data['year'].isin([2021,2022])]
    report1 = data.groupby(['year','quarter','region','segment','category_name']).agg({"quantity":"sum","sales":"sum","profit":"sum","customer_name":pd.Series.nunique})
    report1.to_csv("repo_Cat.csv")
    print(report1)
        
    report2 = data.groupby(['year','quarter','region','ship_mode']).agg({"quantity":"sum","sales":"sum","profit":"sum"})
    report2.to_csv("repo_ship_mode.csv")
    
    data1 = data.dropna()
    report3 = data1.groupby(['year','quarter','region','state']).agg({"quantity":"sum","sales":"sum","profit":"sum","is_returns":"count"})
    report3.to_csv("repo_returned.csv")
    
    
    def exec_report_calculate(df):
        df['profit'] = df['profit'].astype(float)
        df['sales'] = df['sales'].astype(float)
        df['quantity'] = df['quantity'].astype(float)
        df['Quarter on Quarter growth percentage'] = df['profit'].pct_change()
        df['Quarter on Quarter sales'] = df['sales'].pct_change()
        df['Quarter on Quarter quantity'] = df['quantity'].pct_change()
        
        return df
    
    
    exec = data.groupby(['year','quarter','region','regional_manager']).agg({"quantity":"sum","sales":"sum","profit":"sum"}).reset_index()
    exec = exec.groupby(['region','regional_manager']).apply(exec_report_calculate)
    print(exec)
    exec.to_csv("cxoreport.csv")
    
    def exec_report_calculate1(df):
        df['customer_name'] = df['customer_name'].astype(float)
        df['Quarter on Quarter customer growth percentage'] = df['customer_name'].pct_change()
        
        return df
    
    exec1 = data.groupby(['year','quarter','region']).agg({"customer_name":pd.Series.nunique}).reset_index()
    exec1 = exec1.groupby(['region']).apply(exec_report_calculate1)
    print(exec1)
    exec1.to_csv("cxoreport2.csv")
    
    
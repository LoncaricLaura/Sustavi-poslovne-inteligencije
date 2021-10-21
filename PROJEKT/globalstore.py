#Projekt SPI
# -*- coding: utf-8 -*-

# Imports
import pymysql
import pandas as pd
import numpy as np
import json
import requests
import random
from sqlalchemy import create_engine
from datetime import datetime

# Import CSV Sales_Products file
CSV_FILE_PATH = r"C:\Users\Laurica\Desktop\2_GODINA\ljetni semestar\SUSTAVI POSLOVNE INTELIGENCIJE\PROJEKT\Global_Superstore.csv"
df = pd.read_csv(CSV_FILE_PATH, delimiter=',', encoding= 'unicode_escape')
print("CSV size: ", df.shape)


# Print leading rows of dataframe
print(df.head())


#Database connection
user = 'root'
passw = 'root20'
host = 'Localhost'
port = 3306
database = 'globalstore'

mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)
print(mydb)
connection = mydb.connect()

#DDL
customer_ddl = "CREATE TABLE globalstore.customer (id INT NOT NULL, customer_name VARCHAR(45), segment VARCHAR(45), PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC));"
connection.execute(customer_ddl)
product_type_ddl = "CREATE TABLE globalstore.product_type (id INT NOT NULL, category VARCHAR(45), sub_category VARCHAR(45),PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC));"
connection.execute(product_type_ddl)
product_ddl = "CREATE TABLE globalstore.product (id INT NOT NULL PRIMARY KEY, product_name VARCHAR(200) NOT NULL, sales FLOAT , quantity INT, discount FLOAT, product_type_id INT NOT NULL, UNIQUE INDEX id_UNIQUE (id ASC), CONSTRAINT product_type_id FOREIGN KEY (product_type_id) REFERENCES globalstore.product_type (id));"
connection.execute(product_ddl)
location_ddl = "CREATE TABLE globalstore.location (id INT NOT NULL, city VARCHAR(45), state VARCHAR(45), country VARCHAR(45), PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC));"
connection.execute(location_ddl)
market_place_ddl = "CREATE TABLE globalstore.market_place (id INT NOT NULL, market VARCHAR(45), region VARCHAR(45), PRIMARY KEY (id), UNIQUE INDEX id_UNIQUE (id ASC));"
connection.execute(market_place_ddl)
order_ddl = "CREATE TABLE globalstore.order (id VARCHAR(20) NOT NULL, order_date DATETIME, ship_date DATETIME, ship_mode VARCHAR(50), shipping_cost FLOAT, profit FLOAT, order_priority VARCHAR(50), customer_id INT NOT NULL, product_id INT NOT NULL, location_id INT NOT NULL, market_place_id INT NOT NULL, PRIMARY KEY(id), UNIQUE INDEX id_UNIQUE (id ASC),  INDEX customer_id_idx (customer_id ASC), INDEX product_id_idx (product_id ASC), INDEX location_id_idx (location_id ASC), INDEX market_place_idx (market_place_id ASC), CONSTRAINT customer_id    FOREIGN KEY (customer_id)    REFERENCES globalstore.customer (id), CONSTRAINT product_id    FOREIGN KEY (product_id)    REFERENCES globalstore.product (id), CONSTRAINT location_id    FOREIGN KEY (location_id)    REFERENCES globalstore.location (id), CONSTRAINT market_place_id    FOREIGN KEY (market_place_id)    REFERENCES globalstore.market_place (id))"
connection.execute(order_ddl)

#DML
#CUSTOMER
customer_names = df['Customer Name']
customer_segment = []
for i in range (len(customer_names)):
    temp_df = (df['Segment'][i])
    customer_segment.append(temp_df)
customer_data = pd.DataFrame({'id':list(range(1,len(customer_names)+1)),'customer_name':customer_names, 'segment':customer_segment})
customer_data.to_sql(con=mydb, name='customer', if_exists='append', index=False)

#PRODUCT_TYPE
sub_category = []
product_type = df['Category']
for i in range (len(customer_names)):
    tem_df = (df['Sub-Category'][i])
    sub_category.append(tem_df)
product_type_data = pd.DataFrame({'id':list(range(1,len(product_type)+1)),'Category':product_type, 'sub_category':sub_category})
product_type_data.to_sql(con=mydb, name='product_type', if_exists='append', index=False)


#PRODUCT   
product_names = df['Product Name']
product_type_id=[]
for i, row in df.iterrows():
    product_type_id.append(int(product_type_data['id'].iloc[i]))
product_data = pd.DataFrame({'id':list(range(1,len(product_type_id)+1)),'product_name':product_names, 'product_type_id':product_type_id, 'Sales':df['Sales'],'Quantity':df['Quantity'], 'Discount':df['Discount']})
product_data.to_sql(con=mydb, name='product', if_exists='append', index=False)    

#LOCATION
location = df['City']
state, country = [], []
for i in range (len(customer_names)):
    temm_df = (df['State'][i])
    state.append(temm_df)
    temm_df = (df['Country'][i])
    country.append(temm_df)
location_data = pd.DataFrame({'id':list(range(1,len(location)+1)), 'city':location, 'state':state, 'country':country})
location_data.to_sql(con=mydb, name='location', if_exists='append', index=False)

#MARKET PLACE
market_place = df['Market']
region = []
for i in range(len(customer_names)):
    tt_df = (df['Region'][i])
    region.append(tt_df)
market_place_data =  pd.DataFrame({'id':list(range(1,len(market_place)+1)), 'market':market_place, 'region':region})
market_place_data.to_sql(con=mydb, name='market_place', if_exists='append', index=False)

#ORDER
order = []
ship = []
customer_id, product_id, location_id, market_place_id= [], [], [], []
for i, row in df.iterrows():
    order_date = df['Order Date'].iloc[i].split('-')
    order.append(datetime(int(order_date[2]), int(order_date[1]), int(order_date[0])))
    
    ship_date = df['Ship Date'].iloc[i].split('-')
    ship.append(datetime(int(ship_date[2]), int(ship_date[1]), int(ship_date[0])))
    
    customer_id.append(int(customer_data['id'].iloc[i]))
    
    product_id.append(int(product_data['id'].iloc[i]))

    location_id.append(int(location_data['id'].iloc[i]))
    
    market_place_id.append(int(market_place_data['id'].iloc[i]))
    
    
order_data = pd.DataFrame({'id':list(range(1,len(customer_id)+1)),'order_date':order , 'ship_date':ship, 'ship_mode':df['Ship Mode'], 'shipping_cost':df['Shipping Cost'], 'profit':df['Profit'], 'order_priority':df['Order Priority'], 'customer_id':customer_id, 'product_id':product_id, 'location_id':location_id, 'market_place_id':market_place_id})
order_data.to_sql(con=mydb, name='order', if_exists='append', index=False)


x = df['Ship Date'][0]
print((x).split('-'))
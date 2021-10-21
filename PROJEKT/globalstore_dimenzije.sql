DROP DATABASE globalstore_dimenzije;
CREATE DATABASE globalstore_dimenzije;
USE globalstore_dimenzije;

CREATE TABLE dim_Product (
	product_SK INT NOT NULL,
    id INT,
    version INT,
    Product_name VARCHAR(200),
    Sales FLOAT,
    Quantity INT,
    Discount FLOAT,
    Category VARCHAR(45),
    Sub_category VARCHAR(45),
    Date_from DATETIME,
    Date_to DATETIME,
    PRIMARY KEY (product_SK)
    );
    
CREATE TABLE dim_Location (
	location_SK INT NOT NULL,
    id INT,
    version INT,
    City VARCHAR(45),
    State VARCHAR(45),
    Country VARCHAR(45),
    Date_from DATETIME,
    Date_to DATETIME,
    PRIMARY KEY (location_SK)
    );
    
CREATE TABLE dim_Market_place (
	market_place_SK INT NOT NULL,
    id INT,
    version INT,
    Market VARCHAR(45),
    Region VARCHAR(45),
    Date_from DATETIME,
    Date_to DATETIME,
    PRIMARY KEY (market_place_SK)
    );
   
CREATE TABLE dim_Customer (
	customer_SK INT NOT NULL,
    id INT,
    version INT,
    Customer_name VARCHAR(45),
    Segment VARCHAR(45),
	Date_from DATETIME,
    Date_to DATETIME,
    PRIMARY KEY (customer_SK)
    );
 
CREATE TABLE dim_Time_of_delivery (
	time_of_delivery_SK INT NOT NULL,
    id INT,
    order_date DATETIME,
    ship_date DATETIME,
    PRIMARY KEY (time_of_delivery_SK)
    );
    
CREATE TABLE dim_order (
	order_SK INT NOT NULL AUTO_INCREMENT,
    id INT,
    Ship_mode VARCHAR(50),
    Shipping_cost DOUBLE,
    Profit DOUBLE,
    Order_priority VARCHAR(50),
    product_SK INT,
    location_SK INT,
    customer_SK INT,
    time_of_delivery_SK INT,
    market_place_SK INT,
    PRIMARY KEY (order_SK),
    FOREIGN KEY (product_SK) REFERENCES dim_Product(product_SK) ON DELETE NO ACTION ON UPDATE CASCADE,
    FOREIGN KEY (location_SK) REFERENCES dim_Location(location_SK) ON DELETE NO ACTION ON UPDATE CASCADE,
    FOREIGN KEY (customer_SK) REFERENCES dim_Customer(customer_SK) ON DELETE NO ACTION ON UPDATE CASCADE,
    FOREIGN KEY (time_of_delivery_SK) REFERENCES dim_Time_of_delivery(time_of_delivery_SK) ON DELETE NO ACTION ON UPDATE CASCADE,
    FOREIGN KEY (market_place_SK) REFERENCES dim_Market_place(market_place_SK) ON DELETE NO ACTION ON UPDATE CASCADE
    );

    select * from dim_customer;
    
    select * from globalstore_dimenzije.dim_location;
    
    select * from dim_market_place;
    
    select * from dim_product;

	select * from dim_time_of_delivery;

    select * from dim_order;
   


   



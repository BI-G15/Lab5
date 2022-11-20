-- Sentencias utilizadas para eliminar datos entre las pruebas
DELETE FROM fact_order;
DELETE FROM customer;
DELETE FROM city;
DELETE FROM employee;
DELETE FROM date_table;
DELETE FROM stockitem;

-- Sentencias para consultar los datos de dimensiones y tablas de hecho
SELECT * FROM employee;
SELECT * FROM customer;
SELECT * FROM city;
SELECT * FROM fact_order;
SELECT * FROM date_table;
SELECT * FROM stockitem;

-- Sentencias utilizadas para la insercion de datos
INSERT INTO city (City_Key,City,State_Province,Country,
				  Continent,Sales_Territory,Region,
				  Subregion,Latest_Recorded_Population) 
VALUES (2000,'Carrollton','Virginia','United States',
		'North America','Southeast','Americas',
		'Northern America',4574);


INSERT INTO customer (Customer_Key,Customer,Bill_To_Customer,
					  Category,Buying_Group,Primary_Contact,
					  Postal_Code) 
VALUES (2000,'Tailspin Toys (Sylvanite- MT)',
		'Tailspin Toys (Head Office)','Novelty Shop',
		'Tailspin Toys','Lorena Cindric',90216.0);
		

INSERT INTO date_table (Date_key,Day_Number,Day_val,Month_val,
						Short_Month,Calendar_Month_Number,
						Calendar_Year,Fiscal_Month_Number,
						Fiscal_Year) 
VALUES (TO_DATE('2013-01-03','YYYY-MM-DD'),3,3,'January',
		'Jan',1,2013,3,2013);
		

INSERT INTO employee (Employee_Key,Employee,Preferred_Name,
					  Is_Salesperson) 
VALUES (2000,'Isabella Rupp','Isabella','False');


INSERT INTO stockitem (Stock_Item_Key,Stock_Item,Color,Selling_Package,
					   Buying_Package,Brand,Size_val,Lead_Time_Days,
					   Quantity_Per_Outer,Is_Chiller_Stock,Tax_Rate,
					   Unit_Price,Recommended_Retail_Price,
					   Typical_Weight_Per_Unit) 
VALUES (2000,'Void fill 400 L bag (White) 400L','nan','Each','Each',
		'unknown','400L',14,10,False,14.0,50.0,74.75,1.0);


INSERT INTO fact_order (Order_Key,City_Key,Customer_Key,Stock_Item_Key,
						Order_Date_Key,Picked_Date_Key,Salesperson_Key,
						Picker_Key,Package,Quantity,Unit_Price,Tax_Rate,
						Total_Excluding_Tax,Tax_Amount,Total_Including_Tax) 
VALUES (2000,2000,2000,2000,TO_DATE('2015-11-17','YYYY-MM-DD'),
		TO_DATE('2013-07-19','YYYY-MM-DD'),2,133,'S',76,721.71,59,6585.7,
		135.12,8442.06);
		
-- Sentencias para la creacion de tablas
		CREATE TABLE IF NOT EXISTS date_table(
            Date_key DATE PRIMARY KEY,
            Day_Number INT,
            Day_val INT,
            Month_val VARCHAR(20),
            Short_Month VARCHAR(10),
            Calendar_Month_Number INT,
            Calendar_Year INT,
            Fiscal_Month_Number INT,
            Fiscal_Year INT
        );

        CREATE TABLE IF NOT EXISTS city(
            City_Key INT PRIMARY KEY,
            City VARCHAR(150),
            State_Province VARCHAR(150),
            Country VARCHAR(150),
            Continent VARCHAR(150),
            Sales_Territory VARCHAR(150),
            Region VARCHAR(150),
            Subregion VARCHAR(150),
            Latest_Recorded_Population INT
        );

        CREATE TABLE IF NOT EXISTS customer(
            Customer_Key INT PRIMARY KEY,
            Customer VARCHAR(150),
            Bill_To_Customer VARCHAR(150),
            Category VARCHAR(150),
            Buying_Group VARCHAR(150),
            Primary_Contact VARCHAR(150),
            Postal_Code INT
        );

        CREATE TABLE IF NOT EXISTS employee(
            Employee_Key INT PRIMARY KEY,
            Employee VARCHAR(150),
            Preferred_Name VARCHAR(150),
            Is_Salesperson BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS stockitem(
            Stock_Item_Key INT PRIMARY KEY,
            Stock_Item VARCHAR(200),
            Color VARCHAR(50),
            Selling_Package VARCHAR(50),
            Buying_Package VARCHAR(50),
            Brand VARCHAR(50),
            Size_val VARCHAR(50),
            Lead_Time_Days INT,
            Quantity_Per_Outer INT,
            Is_Chiller_Stock BOOLEAN,
            Tax_Rate DECIMAL,
            Unit_Price DECIMAL,
            Recommended_Retail_Price DECIMAL,
            Typical_Weight_Per_Unit DECIMAL
        );


        CREATE TABLE IF NOT EXISTS fact_order(
            Order_Key INT PRIMARY KEY,
            City_Key INT REFERENCES city (city_key),
            Customer_Key INT REFERENCES customer (customer_key),
            Stock_Item_Key INT REFERENCES stockitem (stock_item_key),
            Order_Date_Key DATE REFERENCES date_table (date_key),
            Picked_Date_Key DATE REFERENCES date_table (date_key),
            Salesperson_Key INT REFERENCES employee (employee_key),
            Picker_Key INT REFERENCES employee (employee_key),
            Package VARCHAR(50),
            Quantity INT,
            Unit_Price DECIMAL,
            Tax_Rate DECIMAL,
            Total_Excluding_Tax DECIMAL,
            Tax_Amount DECIMAL,
            Total_Including_Tax DECIMAL
        );

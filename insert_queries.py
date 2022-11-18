from utils.file_util import cargar_datos

# city insertion
def insert_query_city(**kwargs):
    
    insert = f"INSERT INTO city (City_Key,City,State_Province,Country,Continent,Sales_Territory,Region,Subregion,Latest_Recorded_Population) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.City_Key},\'{row.City}\',\'{row.State_Province}\',\'{row.Country}\',\'{row.Continent}\',\'{row.Sales_Territory}\',\'{row.Region}\',\'{row.Subregion}\',{row.Latest_Recorded_Population});\n"
        return insertQuery
    except:
        return ""

# customer insertion
def insert_query_customer(**kwargs):
    insert = f"INSERT INTO customer (Customer_Key,Customer,Bill_To_Customer,Category,Buying_Group,Primary_Contact,Postal_Code) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.Customer_Key},\'{row.Customer}\',\'{row.Bill_To_Customer}\',\'{row.Category}\',\'{row.Buying_Group}\',\'{row.Primary_Contact}\',{row.Postal_Code});\n"
        return insertQuery
    except:
        return ""

# date insertion
def insert_query_date(**kwargs):
    # To Do: recuerden que tratar con variables de tipo "DATE" en sql se hace uso de la instrucción TO_DATE. ejemplo: TO_DATE('31-12-2022','DD-MM-YYYY')
    insert = f"INSERT INTO date_table (Date_key,Day_Number,Day_val,Month_val,Short_Month,Calendar_Month_Number,Calendar_Year,Fiscal_Month_Number,Fiscal_Year) VALUES "
    insertQuery = ""
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(TO_DATE(\'{row.Date_key}\',\'YYYY-MM-DD\'),\'{row.Day_Number}\',\'{row.Day_val}\',\'{row.Month_val}\',\'{row.Short_Month}\',\'{row.Calendar_Month_Number}\',\'{row.Calendar_Year}\',\'{row.Fiscal_Month_Number}\',\'{row.Fiscal_Year});\n"
        return insertQuery
    except:
        return ""

# employee insertion
def insert_query_employee(**kwargs):
    # To Do
    pass

# stock item insertion
def insert_query_stock(**kwargs):
    # To Do
    pass
    
# fact order insert
def insert_query_fact_order(**kwargs):
    # To Do
    pass

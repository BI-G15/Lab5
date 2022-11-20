import pandas as pd
import os

def cargar_datos(name):
    df = pd.read_csv("/opt/airflow/data/" + name + ".csv", sep=',', encoding = 'latin1', index_col=False)
    return df

def guardar_datos(df, nombre):
    df.to_csv("/opt/airflow/data/" + nombre + ".csv" , encoding = 'latin1', sep=',', index=False)  

def quitarErroneos(row):
    if row != "Northwind" :
      return "unknown"
    return row
    
def quitarCaracteresEsp(row):
    return ' '.join(filter(str.isalnum, row))

def volverDecimal(row):
    respuesta = row.replace(",",".")
    if respuesta[0] == ".":
        respuesta = "0" + respuesta 
    return float(respuesta)

# FUNCIONES DE LIMPIEZA

def limpiarCustomer(df):
    # columnas donde quiero remover caracteres:
    columnas = ['Customer', 'Bill_To_Customer']
    df[columnas] = df[columnas].replace({'\'':''}, regex=True)
    return df

def limpiarStock(df):
    columnas = ['Tax_Rate', 'Unit_Price', 'Recommended_Retail_Price','Typical_Weight_Per_Unit']
    df[columnas] = df[columnas].replace({',':'.'}, regex=True)
    df['Tax_Rate'] = df['Tax_Rate'].apply(volverDecimal)
    columnas2 = ['Stock_Item']
    df[columnas2] = df[columnas2].replace({'\'':''}, regex=True)
    return df

    
def procesar_datos():
    
    ## Dimension city
    city = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_city.csv?op=OPEN&user.name=cursobi15", sep=',', encoding = 'latin1', index_col=False) # recuerden cambiar XX por el número de su grupo
    # To Do: Limpiar los datos y guardarlos
    city=city.dropna()
    #Se revisaron los datos numericos y demas columnas y no se encontraron inconsistencias en los datos
    guardar_datos(city, "dimension_city")

    ## Dimension Customer
    customer = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_customer.csv?op=OPEN&user.name=cursobi15", sep=',', encoding = 'latin1', index_col=False)
    # To Do: Limpiar los datos y guardarlos
    customer = customer.dropna()
    customer = limpiarCustomer(customer)
    #Se revisaron los datos numericos y demas columnas y no se encontraron inconsistencias en los datos
    guardar_datos(customer, "dimension_customer")
    
    ## Dimension Date
    date = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_date.csv?op=OPEN&user.name=cursobi15", sep=',', encoding = 'latin1', index_col=False)
    # To Do: Limpiar los datos y guardarlos
    #Los datos de date no contenian nulos
    #Se revisaron los formatos de fechas, datos numericos y demas columnas y no se encontraron inconsistencias en los datos
    guardar_datos(date, "dimension_date")

    ## Dimension Employee
    employee = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_employee.csv?op=OPEN&user.name=cursobi15", sep=',', encoding = 'latin1', index_col=False)
    # To Do: Limpiar los datos y guardarlos
    employee = employee.dropna()
    guardar_datos(employee, "dimension_employee")

    ## Dimension Stock item
    stock_item = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_stock_item.csv?op=OPEN&user.name=cursobi15", sep=',', encoding = 'latin1', index_col=False)
    # To Do: Limpiar los datos y guardarlos
    #en stock_item la columna brand contenia muchos nulos, asi que se remplazaron los nulos con "unknown" para no borrar registros
    stock_item["Brand"]=stock_item["Brand"].apply(quitarErroneos)
    stock_item = limpiarStock(stock_item)
    columnas = ['Stock_Item']
    stock_item[columnas] = stock_item[columnas].replace({'\'':''}, regex=True)
    guardar_datos(stock_item, "dimension_stock_item")
    
    ## Fact Table
    fact_order = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/fact_order.csv?op=OPEN&user.name=cursobi15", sep=',', encoding = 'latin1', index_col=False)
    # To Do: Limpiar los datos y guardarlos
    #Los datos se encuentran sin registros nulos
    #Se revisaron los formatos de fechas, datos numericos y demas columnas y no se encontraron inconsistencias en los datos
    guardar_datos(fact_order, "fact_order")




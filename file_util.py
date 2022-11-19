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
    guardar_datos(stock_item, "dimension_stock_item")
    
    ## Fact Table
    fact_order = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/fact_order.csv?op=OPEN&user.name=cursobi15", sep=',', encoding = 'latin1', index_col=False)
    # To Do: Limpiar los datos y guardarlos
    #Los datos se encuentran sin registros nulos
    #Se revisaron los formatos de fechas, datos numericos y demas columnas y no se encontraron inconsistencias en los datos
    guardar_datos(fact_order, "fact_order")




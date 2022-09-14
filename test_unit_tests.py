from conftest import spark_session
import pytest
import datetime
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,DoubleType,FloatType,DateType,LongType)
from functions import read_files,total_viajes,total_ingresos,metricas,personaConMasKilometros,personaConMasIngresos,percentileN,CodPostalOrigen,CodPostalDestino
from os.path import exists

def test_read_files(spark_session):
    ExpectedResult=read_files()
    schema=StructType(
    [
        StructField('identificador', StringType(), True),
        StructField('codigo_postal_origen', StringType(), True),
        StructField('codigo_postal_destino', StringType(), True),
        StructField('kilometros', FloatType(), True),
        StructField('precio_kilometro', FloatType(), True)
    ]
)
    correct_df = spark_session.createDataFrame([("98","11501","60101",   15.0,794.0),
                                                ("98","11504","10305",10006.0,590.0),
                                                ("98","10305","10456",   12.0, 57.0),
                                                ("98","11501","60101",    9.0,371.0),
                                                ("98","11501","10101",    5.0,483.0),
                                                ("98","10305","10101",    9.0,628.0),
                                                ("98","10305","10101",    6.0,581.0),
                                                ("98","11504","10456",   10.0,671.0),
                                                ("98","11501","60101",   10.0,236.0),
                                                ("98","11504","60101",    3.0,436.0),
                                                ("78","11504","10101",    7.0,884.0),
                                                ("78","10305","10456",   10.0,645.0),
                                                ("78","10305","10456",   14.0,430.0),
                                                ("78","11501","10101",   12.0,157.0),
                                                ("78","10305","10456",   14.0,557.0),
                                                ("78","10305","10456",   14.0,968.0),
                                                ("78","10305","60101",   12.0,203.0),
                                                ("78","10305","10456",    2.0,145.0),
                                                ("78","11504","10456",    4.0,737.0),
                                                ("78","11504","10101",    5.0,567.0),
                                                ("77","11501","10456",   12.0,265.0),
                                                ("77","10305","10101",   12.0,108.0),
                                                ("77","11504","60101",   15.0,140.0),
                                                ("77","10305","60101",    9.0,543.0),
                                                ("77","11501","10456",    5.0,431.0),
                                                ("77","11504","60101",    4.0,533.0),
                                                ("77","10305","10456",   14.0,404.0),
                                                ("77","11501","10101",   11.0, 93.0),
                                                ("77","11501","10456",    9.0,437.0),
                                                ("77","10305","60101",   11.0,189.0),
                                                ("46","11501","10456",    9.0,1000.0),
                                                ("46","11504","60101",    9.0,558.0),
                                                ("46","11504","60101",   15.0,905.0),
                                                ("46","11504","60101",    7.0,234.0),
                                                ("46","10305","10456",   12.0,507.0),
                                                ("46","10305","10456",   11.0,976.0),
                                                ("46","10305","60101",   10.0,  4.0),
                                                ("46","10305","60101",    9.0,526.0),
                                                ("46","11501","60101",    6.0,482.0),
                                                ("46","11504","10101",    4.0,566.0),
                                                ("37","10305","10456",   10.0,457.0),
                                                ("37","11501","60101",    5.0, 78.0),
                                                ("37","10305","60101",   12.0,684.0),
                                                ("37","11504","10456",   15.0,133.0),
                                                ("37","10305","10101",    6.0,321.0),
                                                ("37","11504","60101",    2.0,675.0),
                                                ("37","11504","60101",   12.0,887.0),
                                                ("37","11504","10101",    1.0,522.0),
                                                ("37","11504","10456",   11.0,343.0),
                                                ("37","11504","10101",    7.0, 26.0)],schema=schema)
    assert ExpectedResult.collect() == correct_df.collect()                                           

def test_total_viajes(spark_session):
    datos=read_files()
    viajes=total_viajes(datos)
    schema=StructType(
    [
        StructField('Codigo Postal', StringType(), True),
        StructField('tipo', StringType(), True),
        StructField('Conteo', LongType(), True)
    ]
)
    correct_df = spark_session.createDataFrame([("10305","Origen",20),
                                                ("11504","Origen",18),
                                                ("11501","Origen", 12),
                                                ("10456","Destino",18),
                                                ("60101","Destino",19),
                                                ("10101","Destino",12),
                                                ("10305","Destino",1)
                                                ],schema=schema)
    assert viajes.collect() == correct_df.collect()                             

def test_total_ingresos(spark_session):
    datos=read_files()
    ingresos= total_ingresos(datos)
    schema=StructType(
    [
        StructField('Codigo Postal', StringType(), True),
        StructField('tipo', StringType(), True),
        StructField('Ingreso por Codigo Postal y Tipo', LongType(), True)
    ]
)
    correct_df = spark_session.createDataFrame([("10305","Origen",  96584),
                                                ("11504","Origen",5968726),
                                                ("11501","Origen",  44481),
                                                ("10456","Destino",  95534),
                                                ("60101","Destino",  81044),
                                                ("10101","Destino",  29673),
                                                ("10305","Destino",5903540)
                                                ],schema=schema)
    assert ingresos.collect() == correct_df.collect() 

def test_metricas(spark_session):
    datos=read_files()
    metricass= metricas(datos)
    schema=StructType(
    [
        StructField('Tipo de Metrica', StringType(), True),
        StructField('Valor', StringType(), True)
        
    ]
)
    correct_df = spark_session.createDataFrame([("persona_con_mas_kilometros","98"),
                                                ("persona_con_mas_ingresos","98"),
                                                ("percentil_25","1926.0"),
                                                ("percentil_50","2948.0"),
                                                ("percentil_75","6084.0"),
                                                ("codigo_postal_origen_con_mas_ingresos","11504"),
                                                ("codigo_postal_destino_con_mas_ingresos","10305")
                                                ],schema=schema)
    assert metricass.collect() == correct_df.collect() 

def test_personaConMasKilometros(spark_session):
    datos=read_files()
    persona= personaConMasKilometros(datos)
    schema=StructType(
    [
        StructField('identificador', StringType(), True),
        StructField('Metrica', DoubleType(), True)
        
    ]
)
    correct_df = spark_session.createDataFrame([("98",10085.0)
                                                ],schema=schema)
    assert persona.collect() == correct_df.collect() 


def test_personaConMasIngresos(spark_session):
    datos=read_files()
    persona= personaConMasIngresos(datos)
    schema=StructType(
    [
        StructField('identificador', StringType(), True),
        StructField('Metrica', DoubleType(), True)
        
    ]
)
    correct_df = spark_session.createDataFrame([("98",5941404.0)
                                                ],schema=schema)
    assert persona.collect() == correct_df.collect() 

def test_percentile_25(spark_session):
    datos=read_files()
    persona= percentileN(datos,.25,"Percentile_25")
    schema=StructType(
    [
        StructField('value', StringType(), True),
        StructField('Identificador', StringType(), True)
        
    ]
)
    correct_df = spark_session.createDataFrame([("1926.0","Percentile_25")
                                                ],schema=schema)
    assert persona.collect() == correct_df.collect() 

def test_percentile_50(spark_session):
    datos=read_files()
    persona= percentileN(datos,.50,"Percentile_50")
    schema=StructType(
    [
        StructField('value', StringType(), True),
        StructField('Identificador', StringType(), True)
        
    ]
)
    correct_df = spark_session.createDataFrame([("2948.0","Percentile_50")
                                                ],schema=schema)
    assert persona.collect() == correct_df.collect() 

def test_percentile_75(spark_session):
    datos=read_files()
    persona= percentileN(datos,.75,"Percentile_25")
    schema=StructType(
    [
       
        StructField('value', StringType(), True),
        StructField('Identificador', StringType(), True)
        
    ]
)
    correct_df = spark_session.createDataFrame([("6084.0","Percentile_25")
                                                ],schema=schema)
    assert persona.collect() == correct_df.collect() 

def test_CodPostalOrigen(spark_session):
    datos=read_files()
    Oringen= CodPostalOrigen(datos)
    schema=StructType(
    [
       
        StructField('codigo_postal_origen', StringType(), True),
        StructField('Metrica', DoubleType(), True)
        
    ]
)
    correct_df = spark_session.createDataFrame([("11504",5968726.0)
                                                ],schema=schema)
    assert Oringen.collect() == correct_df.collect() 
def test_CodPostalDestino(spark_session):
    datos=read_files()
    Oringen= CodPostalDestino(datos)
    schema=StructType(
    [
       
        StructField('codigo_postal_destino', StringType(), True),
        StructField('Metrica', DoubleType(), True)
        
    ]
)
    correct_df = spark_session.createDataFrame([("10305",5903540.0)
                                                ],schema=schema)
    assert Oringen.collect() == correct_df.collect() 

def test_viajes_file(spark_session):
    file_exists = exists("resultados/total_viajes")
    assert True ==file_exists
def test_ingresos_file(spark_session):
    file_exists = exists("resultados/total_ingresos")
    assert True ==file_exists
def test_metricas_file(spark_session):
    file_exists = exists("resultados/metricas")
    assert True ==file_exists
def test_base_file_exists(spark_session):
    file_exists = exists("resultados/ArchivoNoExite")
    assert False ==file_exists
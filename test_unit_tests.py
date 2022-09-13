from conftest import spark_session
import pytest
import datetime
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType,LongType)
from functions import read_files,total_viajes,total_ingresos,metricas
from os.path import exists

def test_read_files(spark_session):
    ExpectedResult=read_files()
    schema=StructType(
    [
        StructField('identificador', StringType(), True),
        StructField('codigo_postal_origen', StringType(), True),
        StructField('codigo_postal_destino', StringType(), True),
        StructField('kilometros', IntegerType(), True),
        StructField('precio_kilometro', IntegerType(), True)
    ]
)
    correct_df = spark_session.createDataFrame([("98","11501","60101",   15,794),
                                                ("98","11504","10305",10006,590),
                                                ("98","10305","10456",   12, 57),
                                                ("98","11501","60101",    9,371),
                                                ("98","11501","10101",    5,483),
                                                ("98","10305","10101",    9,628),
                                                ("98","10305","10101",    6,581),
                                                ("98","11504","10456",   10,671),
                                                ("98","11501","60101",   10,236),
                                                ("98","11504","60101",    3,436),
                                                ("78","11504","10101",    7,884),
                                                ("78","10305","10456",   10,645),
                                                ("78","10305","10456",   14,430),
                                                ("78","11501","10101",   12,157),
                                                ("78","10305","10456",   14,557),
                                                ("78","10305","10456",   14,968),
                                                ("78","10305","60101",   12,203),
                                                ("78","10305","10456",    2,145),
                                                ("78","11504","10456",    4,737),
                                                ("78","11504","10101",    5,567),
                                                ("77","11501","10456",   12,265),
                                                ("77","10305","10101",   12,108),
                                                ("77","11504","60101",   15,140),
                                                ("77","10305","60101",    9,543),
                                                ("77","11501","10456",    5,431),
                                                ("77","11504","60101",    4,533),
                                                ("77","10305","10456",   14,404),
                                                ("77","11501","10101",   11, 93),
                                                ("77","11501","10456",    9,437),
                                                ("77","10305","60101",   11,189),
                                                ("46","11501","10456",    9,1000),
                                                ("46","11504","60101",    9,558),
                                                ("46","11504","60101",   15,905),
                                                ("46","11504","60101",    7,234),
                                                ("46","10305","10456",   12,507),
                                                ("46","10305","10456",   11,976),
                                                ("46","10305","60101",   10,  4),
                                                ("46","10305","60101",    9,526),
                                                ("46","11501","60101",    6,482),
                                                ("46","11504","10101",    4,566),
                                                ("37","10305","10456",   10,457),
                                                ("37","11501","60101",    5, 78),
                                                ("37","10305","60101",   12,684),
                                                ("37","11504","10456",   15,133),
                                                ("37","10305","10101",    6,321),
                                                ("37","11504","60101",    2,675),
                                                ("37","11504","60101",   12,887),
                                                ("37","11504","10101",    1,522),
                                                ("37","11504","10456",   11,343),
                                                ("37","11504","10101",    7, 26)],schema=schema)
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
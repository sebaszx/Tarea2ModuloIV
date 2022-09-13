from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType)
import sys
from functions import read_files,total_viajes,total_ingresos,metricas
from os.path import exists
#inputs



#spark session

df=read_files()
#df.printSchema()
#df.show(50)

viajes=total_viajes(df)
#viajes.printSchema()
#viajes.show(50)

ingresos= total_ingresos(df)
#ingresos.printSchema()
#ingresos.show(50)

metricass = metricas(df)
metricass.printSchema()
metricass.show(50,Truncate=False)

file_exists = exists("resultados/total_viajes")
print(file_exists)

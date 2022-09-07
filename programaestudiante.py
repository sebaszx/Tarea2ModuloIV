from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType)
import sys
from functions import read_files,total_viajes
#inputs



#spark session

df=read_files()
df.printSchema()
#df.show(50)

viajes=total_viajes(df)
viajes.show(50)
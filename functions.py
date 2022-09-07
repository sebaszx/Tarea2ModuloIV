
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType)
from pyspark.sql.functions import explode,col,lit,col,dense_rank, sum,mean,avg, row_number,count
spark = SparkSession.builder \
    .master("local") \
    .appName("tarea2")  \
    .getOrCreate()
def writeJson(dataframe,nombre):
    dataframe.write.mode('Overwrite').json("resultados/{}".format(nombre))


def read_files():
    """schema=StructType(
    [
        StructField('identificador', StringType(), True),
        StructField('codigo_postal_origen', StringType(), True),
        StructField('codigo_postal_destino', StringType(), True),
        StructField('kilometnanoros', StringType(), True),
        StructField('precio_kilometro', StringType(), True)
    ]
)"""

# Read multiline json file
    multiline_df = spark.read.option("multiline","true") \
        .json("persona/persona*.json")
    multiline_df=multiline_df.select("identificador",explode("viajes").alias("viajes"))
    multiline_df= multiline_df.select(
        col("identificador"),
        col("viajes.codigo_postal_origen").cast(StringType()).alias("codigo_postal_origen"),
        col("viajes.codigo_postal_destino").cast(StringType()).alias("codigo_postal_destino"),
        col("viajes.kilometros").cast(IntegerType()).alias("kilometros"),
        col("viajes.precio_kilometro").cast(IntegerType()).alias("precio_kilometro")
    )
    return multiline_df
"""
total_viajes.csv: contiene 3 columnas que representan 1) el código postal, 2) si es origen
o destino y 3) la cantidad total de viajes para ese código postal como destino u origen.
"""
def total_viajes(dataframe):
    origendataframe= dataframe.select("codigo_postal_origen")
    origendataframe=origendataframe.withColumn("Tipo", lit("Origen"))


    destinodataframe= dataframe.select("codigo_postal_destino")
    destinodataframe=destinodataframe.withColumn("Tipo", lit("Destino"))


    df=origendataframe.union(destinodataframe).withColumnRenamed("codigo_postal_origen", "Codigo Postal")

    df=df.groupBy("Codigo Postal","tipo").count()
    writeJson(df,"total_viajes")
    return df

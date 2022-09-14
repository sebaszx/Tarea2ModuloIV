
import queue
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType)
from pyspark.sql.functions import explode,col,lit,col,dense_rank, sum,mean,avg, row_number,count,ntile,percent_rank,percentile_approx
from pyspark.sql.window import Window
spark = SparkSession.builder \
    .master("local") \
    .appName("tarea2")  \
    .getOrCreate()

def writeJson(dataframe,nombre):
    dataframe.write.mode('Overwrite').csv("resultados/{}".format(nombre))


def read_files():
    """schema=StructType(
    [
        StructField('identificador', StringType(), True),
        StructField('codigo_postal_origen', StringType(), True),
        StructField('codigo_postal_destino', StringType(), True),
        StructField('kilometros', IntegerType(), True),
        StructField('precio_kilometro', IntegerType(), True)
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
        col("viajes.kilometros").cast(FloatType()).alias("kilometros"),
        col("viajes.precio_kilometro").cast(FloatType()).alias("precio_kilometro")
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

    df=df.groupBy("Codigo Postal","tipo").agg(count(lit(1)).alias("Conteo"))
    writeJson(df,"total_viajes")
    return df


"""total_ingresos.csv: contiene 3 columnas que representan 1) el código postal, 2) si es 
origen o destino y 3) la cantidad de dinero generado en ingresos para ese código postal 
como destino u origen."""
def total_ingresos(dataframe):
    origendataframe= dataframe.select("codigo_postal_origen","kilometros","precio_kilometro")
    origendataframe=origendataframe.withColumn("Ingreso", origendataframe.kilometros * origendataframe.precio_kilometro)
    origendataframe=origendataframe.withColumn("Tipo", lit("Origen"))


    destinodataframe= dataframe.select("codigo_postal_destino","kilometros","precio_kilometro")
    destinodataframe=destinodataframe.withColumn("Ingreso", destinodataframe.kilometros * destinodataframe.precio_kilometro)
    destinodataframe=destinodataframe.withColumn("Tipo", lit("Destino"))


    df=origendataframe.union(destinodataframe).withColumnRenamed("codigo_postal_origen", "Codigo Postal")

    df=df.groupBy("Codigo Postal","tipo").agg(sum("Ingreso").alias("Ingreso por Codigo Postal y Tipo"))
    writeJson(df,"total_ingresos")
    return df
def personaConMasKilometros(dataframe):
    #dataframe.printSchema()
    return dataframe.groupBy("identificador").agg(sum("kilometros").alias("Metrica")).orderBy(col("Metrica").desc()).limit(1)

def personaConMasIngresos(dataframe):
    #dataframe.printSchema()
   
    dataframe=dataframe.withColumn("Ingreso", dataframe.kilometros * dataframe.precio_kilometro)
    return dataframe.groupBy("identificador").agg(sum("Ingreso").alias("Metrica")).orderBy(col("Metrica").desc()).limit(1)

def percentileN(dataframe,N,name):
  
    dataframe=dataframe.withColumn("Ingreso", dataframe.kilometros * dataframe.precio_kilometro)
    #windowSpec= Window.orderBy("Ingreso")
    #quantiles = dataframe.approxQuantile("Ingreso", [0.25, 0.5, 0.75], 0)
    quantiles = dataframe.approxQuantile("Ingreso", [N], 0)
    #print("percentil usando approxQuantile",quantiles)
   # df=dataframe.groupBy("Identificador").agg(percentile_approx("Ingreso", N, lit(1000000)).alias(name))
   # print("percentil usando percentile_approx")
   # df.show()
    dataframe=spark.createDataFrame(quantiles, StringType())
    #dataframe.show()
    #dataframe=dataframe.withColumn("value",col("value").cast(StringType()))# se castea a string para que no de error al mo
    #dataframe.show()
    dataframe=dataframe.withColumn("Identificador", lit(name))
    return dataframe
def CodPostalOrigen(dataframe):
    dataframe=dataframe.withColumn("Ingreso", dataframe.kilometros * dataframe.precio_kilometro)
    return dataframe.groupBy("codigo_postal_origen").agg(sum("Ingreso").alias("Metrica")).orderBy(col("Metrica").desc()).limit(1)

def CodPostalDestino(dataframe):
    dataframe=dataframe.withColumn("Ingreso", dataframe.kilometros * dataframe.precio_kilometro)
    return dataframe.groupBy("codigo_postal_destino").agg(sum("Ingreso").alias("Metrica")).orderBy(col("Metrica").desc()).limit(1)

def metricas(dataframe):
    personaKilometros=personaConMasKilometros(dataframe)
    #personaKilometros.show()
    personaIngresos=personaConMasIngresos(dataframe)
    #personaIngresos.printSchema()
    #personaIngresos.show() 
    percentile_25=percentileN(dataframe,.25,"Percentile_25")
    #percentile_25.printSchema()
    #percentile_25.show()
    percentile_50=percentileN(dataframe,.50,"Percentile_50")
    #percentile_50.printSchema()
    #percentile_50.show()
    percentile_75=percentileN(dataframe,.75,"Percentile_75")
    #percentile_75.printSchema()
    #percentile_75.show()
    CodigoPostalOrigen=CodPostalOrigen(dataframe)
    #CodigoPostalOrigen.show()
    #CodigoPostalOrigen.printSchema()
    CodigoPostalDestino=CodPostalDestino(dataframe)
    #CodigoPostalDestino.show()
    #CodigoPostalDestino.printSchema()
    schema=StructType(
    [
        StructField('"Tipo de Metrica"', StringType(), True),
        StructField('Valor', StringType(), True)
    ])
    # test = percentile_25.select(percentile_25.value).rdd.flatMap(lambda x: x).collect()[0]
    # print(test)
    df = spark.createDataFrame(
    [
        ("persona_con_mas_kilometros", personaKilometros.select(personaKilometros.identificador).rdd.flatMap(lambda x: x).collect()[0]),
        ("persona_con_mas_ingresos",personaIngresos.select(personaIngresos.identificador).rdd.flatMap(lambda x: x).collect()[0]), 
        ("percentil_25", percentile_25.select(percentile_25.value).rdd.flatMap(lambda x: x).collect()[0]), 
        ("percentil_50", percentile_50.select(percentile_50.value).rdd.flatMap(lambda x: x).collect()[0]), 
        ("percentil_75",  percentile_75.select(percentile_75.value).rdd.flatMap(lambda x: x).collect()[0]), 
        ("codigo_postal_origen_con_mas_ingresos",CodigoPostalOrigen.select(CodigoPostalOrigen.codigo_postal_origen).rdd.flatMap(lambda x: x).collect()[0]), 
        ("codigo_postal_destino_con_mas_ingresos", CodigoPostalDestino.select(CodigoPostalDestino.codigo_postal_destino).rdd.flatMap(lambda x: x).collect()[0]) 
    ],
        ["Tipo de Metrica", "Valor"])
    writeJson(df,"metricas")
    return df



  
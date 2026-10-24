from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as suma
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DecimalType
)

spark = (
    SparkSession.builder
    .appName("Tarea3_Streaming_Ventas")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

esquema = StructType([
    StructField("producto", StringType()),
    StructField("cantidad", IntegerType()),
    StructField("precio", DecimalType(12, 2))
])

kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ventas_tiempo_real")
    .option("startingOffsets", "earliest")
    .load()
)

ventas = (
    kafka.select(
        from_json(col("value").cast("string"), esquema)
        .alias("venta")
    )
    .select("venta.*")
    .filter(
        col("producto").isNotNull() &
        (col("cantidad") > 0) &
        (col("precio") > 0)
    )
    .withColumn(
        "TotalVenta",
        col("cantidad") * col("precio")
    )
)

resumen = ventas.groupBy("producto").agg(
    suma("cantidad").alias("Unidades"),
    suma("TotalVenta").alias("Ingresos")
)

consulta = (
    resumen.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start()
)

print("Streaming activo. Presiona Ctrl+C para terminar.")
consulta.awaitTermination()

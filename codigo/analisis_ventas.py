from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, countDistinct,
    desc, round as spark_round
)
from pyspark.sql.types import DecimalType

spark = (
    SparkSession.builder
    .appName("Tarea3_Analisis_Comercial")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Cargar datos clasificados
ventas = spark.read.parquet(str(BASE / "datos" / "ventas_limpias.parquet"))
revision = spark.read.parquet(str(BASE / "datos" / "registros_revision.parquet"))

print("\n===== VERIFICACION DE REGISTROS =====")

validos = ventas.count()
excluidos = revision.count()

print("Ventas positivas:", validos)
print("Registros para revision:", excluidos)
print("Total:", validos + excluidos)

# Ajustar precision monetaria
ventas = ventas.withColumn(
    "TotalVenta",
    (
        col("Quantity").cast(DecimalType(18, 0)) *
        col("UnitPrice").cast(DecimalType(18, 2))
    ).cast(DecimalType(22, 2))
)

print("\n===== INDICADORES GENERALES =====")

ventas.agg(
    spark_sum("Quantity").alias("UnidadesVendidas"),
    spark_sum("TotalVenta").alias("IngresosBrutos"),
    countDistinct("InvoiceNo").alias("Facturas")
).show(truncate=False)

print("\n===== CINCO PRODUCTOS MAS VENDIDOS =====")

ventas.groupBy("StockCode", "Description").agg(
    spark_sum("Quantity").alias("UnidadesVendidas"),
    spark_round(
        spark_sum("TotalVenta"), 2
    ).alias("Ingresos")
).orderBy(
    desc("UnidadesVendidas")
).show(5, truncate=False)

spark.stop()

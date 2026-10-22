from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, sum as spark_sum,
    countDistinct, round as spark_round
)
from pyspark.sql.types import DecimalType

spark = (
    SparkSession.builder
    .appName("Tarea3_Ventas_Mensuales")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

ventas = spark.read.parquet(
    str(BASE / "datos" / "ventas_limpias.parquet")
)

ventas = ventas.withColumn(
    "Mes",
    date_format(col("InvoiceDate"), "yyyy-MM")
).withColumn(
    "Importe",
    (
        col("Quantity").cast(DecimalType(18, 0)) *
        col("UnitPrice").cast(DecimalType(18, 2))
    ).cast(DecimalType(22, 2))
)

resumen = ventas.groupBy("Mes").agg(
    spark_sum("Quantity").alias("Unidades"),
    spark_round(
        spark_sum("Importe"), 2
    ).alias("Ingresos"),
    countDistinct("InvoiceNo").alias("Facturas")
).orderBy("Mes")

print("\n===== VENTAS POR MES =====")
resumen.show(24, truncate=False)

resumen.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(BASE / "datos" / "resultados_mensuales"))

print("\nResultados mensuales guardados.")

spark.stop()

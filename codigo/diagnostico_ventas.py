from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = (
    SparkSession.builder
    .appName("Tarea3_Diagnostico_Ventas")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

ruta = str(BASE / "datos" / "online_retail.csv")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .csv(ruta)
)

print("\n===== REGISTROS TOTALES =====")
print(df.count())

print("\n===== CANTIDADES NEGATIVAS =====")
print(df.filter(col("Quantity") < 0).count())

print("\n===== CANTIDADES IGUALES A CERO =====")
print(df.filter(col("Quantity") == 0).count())

print("\n===== PRECIOS MENORES O IGUALES A CERO =====")
print(df.filter(col("UnitPrice") <= 0).count())

print("\n===== FACTURAS DE CANCELACION =====")
print(
    df.filter(
        trim(col("InvoiceNo").cast("string")).startswith("C")
    ).count()
)

print("\n===== EJEMPLOS DE CANCELACIONES =====")
df.filter(
    trim(col("InvoiceNo").cast("string")).startswith("C")
).show(5, truncate=False)

spark.stop()

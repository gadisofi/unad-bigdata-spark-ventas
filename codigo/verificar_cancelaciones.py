from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = (
    SparkSession.builder
    .appName("VerificarCancelaciones")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(BASE / "datos" / "online_retail.csv"))
)

cancelada = trim(col("InvoiceNo").cast("string")).startswith("C")
negativa = col("Quantity") < 0

print("===== CANCELADAS CON CANTIDAD NEGATIVA =====")
print(df.filter(cancelada & negativa).count())

print("===== NEGATIVAS SIN PREFIJO C =====")
print(df.filter(negativa & ~cancelada).count())

print("===== PREFIJO C SIN CANTIDAD NEGATIVA =====")
print(df.filter(cancelada & ~negativa).count())

print("===== EJEMPLOS NEGATIVOS SIN PREFIJO C =====")
df.filter(negativa & ~cancelada).show(10, truncate=False)

spark.stop()

from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Tarea3_Analisis_Ventas_UNAD")
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
    .option("encoding", "UTF-8")
    .csv(ruta)
)

print("\n===== TOTAL DE REGISTROS =====")
print(df.count())

print("\n===== ESTRUCTURA DE LOS DATOS =====")
df.printSchema()

print("\n===== PRIMEROS 10 REGISTROS =====")
df.show(10, truncate=False)

print("\n===== VALORES NULOS POR COLUMNA =====")
from pyspark.sql.functions import col, sum as spark_sum

df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show(truncate=False)

spark.stop()

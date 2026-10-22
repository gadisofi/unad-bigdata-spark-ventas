from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

spark = (
    SparkSession.builder
    .appName("Tarea3_Limpieza_Ventas")
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
    .csv(ruta)
)

print("\n===== REGISTROS ORIGINALES =====")
originales = df.count()
print(originales)

# Condiciones para identificar ventas positivas
condicion = (
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0) &
    (~upper(trim(col("InvoiceNo").cast("string"))).startswith("C")) &
    (col("Description").isNotNull()) &
    (trim(col("Description")) != "")
)

# Separar ventas positivas y registros para revision
ventas = df.filter(condicion)
revision = df.filter(~condicion)

# Calcular valor de cada linea de venta
ventas = ventas.withColumn(
    "TotalVenta",
    col("Quantity") * col("UnitPrice")
)

print("\n===== VENTAS POSITIVAS =====")
validos = ventas.count()
print(validos)

print("\n===== REGISTROS PARA REVISION =====")
excluidos = revision.count()
print(excluidos)

print("\n===== COMPROBACION =====")
print("Originales:", originales)
print("Clasificados:", validos + excluidos)

print("\n===== EJEMPLOS DE VENTAS =====")
ventas.select(
    "InvoiceNo",
    "Description",
    "Quantity",
    "UnitPrice",
    "TotalVenta"
).show(10, truncate=False)

# Guardar resultados para posteriores analisis
ventas.write.mode("overwrite").parquet(
    str(BASE / "datos" / "ventas_limpias.parquet")
)

revision.write.mode("overwrite").parquet(
    str(BASE / "datos" / "registros_revision.parquet")
)

print("\nArchivos procesados y guardados.")

spark.stop()

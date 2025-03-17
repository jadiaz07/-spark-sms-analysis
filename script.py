from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, when, md5, concat_ws
import matplotlib.pyplot as plt 
import pandas as pd
import os

# Crear la carpeta output si no existe
if not os.path.exists("output"):
    os.makedirs("output")
    
# 1. Inicializar sesión de Spark
spark = SparkSession.builder \
    .appName("SMS Billing Analysis") \
    .getOrCreate()

# 2. Cargar los datasets
df_eventos = spark.read.csv("eventos.csv.gz", header=True, inferSchema=True)
df_free_sms = spark.read.csv("free_sms_destinations.csv.gz", header=True, inferSchema=True)

# 3. Limpiar datos: eliminar valores nulos
df_eventos = df_eventos.dropna(subset=["id_source", "id", "region", "sms"])
df_free_sms = df_free_sms.dropna(subset=["id"])

# 4. Unir dataset de eventos con destinos gratuitos
df = df_eventos.join(df_free_sms, df_eventos.id == df_free_sms.id, "left_outer")

# 5. Calcular la facturación
df = df.withColumn(
    "billing",
    when(col("id").isNotNull(), lit(0.0))  # Destinos gratuitos
    .when(col("region").between(1, 5), col("sms") * lit(1.5))  # Regiones 1-5
    .when(col("region").between(6, 9), col("sms") * lit(2.0))  # Regiones 6-9
    .otherwise(lit(0.0))  # Otras regiones
)

# 6. Calcular el monto total de facturación
total_facturacion = df.select(sum("billing")).collect()[0][0]
print(f"Monto total de facturación: ${total_facturacion}")

# 7. Obtener los 100 usuarios con mayor facturación
top_100_users = df.groupBy("id_source") \
    .agg(sum("billing").alias("total_billing")) \
    .orderBy(col("total_billing").desc()) \
    .limit(100)

# 8. Generar ID hasheado con MD5
top_100_users = top_100_users.withColumn("hashed_id", md5(concat_ws("_", col("id_source").cast("string"))))

# 9. Guardar en formato Parquet con compresión GZIP
top_100_users.write.mode("overwrite") \
    .option("compression", "gzip") \
    .parquet("output/top_100_users.parquet")

# 10. Crear histograma de cantidad de llamadas por hora del día
df_calls = df_eventos.groupBy("hour").agg(sum("calls").alias("total_calls"))
pandas_df = df_calls.toPandas()

plt.figure(figsize=(10,6))
plt.bar(pandas_df["hour"], pandas_df["total_calls"], color="blue")
plt.xlabel("Hora del día")
plt.ylabel("Cantidad de llamadas")
plt.title("Distribución de llamadas por hora del día")
plt.xticks(range(0, 24))
plt.grid(True)
plt.savefig("output/histograma_llamadas.png")
plt.show()

# Finalizar sesión de Spark
spark.stop()
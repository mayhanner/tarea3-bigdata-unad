from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count

spark = SparkSession.builder \
    .appName("BatchProcessing_SupermarketSales") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_raw = spark.read.csv("/home/vboxuser/sales.csv", header=True, inferSchema=True)

print("--- 1. DATOS CRUDOS (RAW) ---")
print(f"Total de registros: {df_raw.count()}")
df_raw.show(5)

df_clean = df_raw.dropna(subset=["sale_id", "total_price", "city"])

print("--- 2. DATOS LIMPIOS Y TRANSFORMADOS ---")
print(f"Registros después de limpieza: {df_clean.count()}")
df_clean.show(5)

print("--- 3. INGRESOS TOTALES POR CIUDAD ---")
df_city = df_clean.groupBy("city") \
                  .agg(sum("total_price").alias("Ingresos_Totales"),
                       count("sale_id").alias("Num_Ventas")) \
                  .orderBy(col("Ingresos_Totales").desc())
df_city.show()

print("--- 4. VENTAS PROMEDIO POR CATEGORÍA ---")
df_product = df_clean.groupBy("product_category") \
                     .agg(avg("total_price").alias("Venta_Promedio")) \
                     .orderBy(col("Venta_Promedio").desc())
df_product.show()

print("--- 5. VENTAS POR TIPO DE CLIENTE Y GÉNERO ---")
df_customer = df_clean.groupBy("customer_type", "gender") \
                      .agg(avg("total_price").alias("Ticket_Promedio")) \
                      .orderBy("customer_type", "gender")
df_customer.show()

spark.stop()
print("--- PROCESAMIENTO EN BATCH COMPLETADO ---")
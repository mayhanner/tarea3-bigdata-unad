"""
Tarea 3 - Big Data (UNAD) - Procesamiento en BATCH con Apache Spark (PySpark)
Dataset: ventas de supermercado (sales.csv)

Fases:
  1. Carga de datos crudos desde CSV
  2. Limpieza y transformación (nulos, duplicados, tipos, valores inválidos, columnas derivadas)
  3. Análisis exploratorio (EDA) con DataFrames + un ejemplo con RDD
  4. Visualización: gráficos PNG guardados en la carpeta de salida

Uso:
  spark-submit batch_processing.py [ruta_csv] [carpeta_salida]
  Ejemplo: /opt/Spark/bin/spark-submit batch_processing.py /home/vboxuser/sales.csv /home/vboxuser/salida_batch
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA_CSV = sys.argv[1] if len(sys.argv) > 1 else "/home/vboxuser/sales.csv"
DIR_SALIDA = sys.argv[2] if len(sys.argv) > 2 else "/home/vboxuser/salida_batch"
os.makedirs(DIR_SALIDA, exist_ok=True)

spark = SparkSession.builder.appName("BatchProcessing_SupermarketSales").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. CARGA DE DATOS CRUDOS
# ---------------------------------------------------------------------------
print("\n--- 1. DATOS CRUDOS (RAW) ---")
df_raw = spark.read.csv(RUTA_CSV, header=True, inferSchema=True)
total_raw = df_raw.count()
print(f"Total de registros: {total_raw}")
print("Esquema inferido:")
df_raw.printSchema()
df_raw.show(5)

# ---------------------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACIÓN
# ---------------------------------------------------------------------------
print("--- 2. LIMPIEZA Y TRANSFORMACIÓN ---")

# 2.1 Conteo de nulos por columna (diagnóstico previo a la limpieza)
print("Nulos por columna (antes de limpiar):")
df_raw.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_raw.columns]).show()

df_clean = (
    df_raw
    # 2.2 Eliminar registros sin los campos clave
    .dropna(subset=["sale_id", "total_price", "city"])
    # 2.3 Eliminar duplicados exactos y duplicados por identificador de venta
    .dropDuplicates()
    .dropDuplicates(["sale_id"])
    # 2.4 Normalizar tipos de datos
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("total_price", F.col("total_price").cast("double"))
    # 2.5 Normalizar texto (espacios y mayúsculas)
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("product_category", F.initcap(F.trim(F.col("product_category"))))
    # 2.6 Filtrar valores inválidos (precios o cantidades <= 0)
    .filter((F.col("unit_price") > 0) & (F.col("quantity") > 0) & (F.col("total_price") > 0))
    # 2.7 Columna derivada: subtotal antes de impuestos
    .withColumn("subtotal", F.round(F.col("unit_price") * F.col("quantity"), 2))
)

# cache(): el DataFrame limpio se reutiliza en varias agregaciones y evita recalcular el plan
df_clean.cache()
total_clean = df_clean.count()
print(f"Registros después de limpieza: {total_clean} (descartados: {total_raw - total_clean})")
df_clean.show(5)

# ---------------------------------------------------------------------------
# 3. ANÁLISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------------------
print("--- 3.1 ESTADÍSTICAS DESCRIPTIVAS ---")
df_clean.select("unit_price", "quantity", "total_price").describe().show()

print("--- 3.2 INGRESOS TOTALES POR CIUDAD ---")
df_city = (
    df_clean.groupBy("city")
    .agg(F.round(F.sum("total_price"), 2).alias("Ingresos_Totales"),
         F.count("sale_id").alias("Num_Ventas"))
    .orderBy(F.col("Ingresos_Totales").desc())
)
df_city.show()

print("--- 3.3 VENTA PROMEDIO POR CATEGORÍA DE PRODUCTO ---")
df_product = (
    df_clean.groupBy("product_category")
    .agg(F.round(F.avg("total_price"), 2).alias("Venta_Promedio"),
         F.count("sale_id").alias("Num_Ventas"))
    .orderBy(F.col("Venta_Promedio").desc())
)
df_product.show()

print("--- 3.4 TICKET PROMEDIO POR TIPO DE CLIENTE Y GÉNERO ---")
df_customer = (
    df_clean.groupBy("customer_type", "gender")
    .agg(F.round(F.avg("total_price"), 2).alias("Ticket_Promedio"),
         F.count("sale_id").alias("Num_Ventas"))
    .orderBy("customer_type", "gender")
)
df_customer.show()

# 3.5 Ejemplo con RDD (map + reduceByKey + take): unidades vendidas por categoría
print("--- 3.5 EJEMPLO CON RDD: UNIDADES VENDIDAS POR CATEGORÍA ---")
rdd_unidades = (
    df_clean.select("product_category", "quantity").rdd
    .map(lambda fila: (fila["product_category"], fila["quantity"]))   # transformación
    .reduceByKey(lambda a, b: a + b)                                  # transformación
    .sortBy(lambda par: par[1], ascending=False)                      # transformación
)
for categoria, unidades in rdd_unidades.take(10):                     # acción
    print(f"{categoria:<18} {unidades}")

# ---------------------------------------------------------------------------
# 4. VISUALIZACIÓN (los agregados son pequeños: se pasan a pandas para graficar)
# ---------------------------------------------------------------------------
print("\n--- 4. VISUALIZACIÓN ---")
try:
    import matplotlib
    matplotlib.use("Agg")  # sin entorno gráfico (servidor por SSH)
    import matplotlib.pyplot as plt

    p_city = df_city.toPandas()
    p_prod = df_product.toPandas()
    p_cust = df_customer.toPandas()
    p_cust["segmento"] = p_cust["customer_type"] + " - " + p_cust["gender"]

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(p_city["city"], p_city["Ingresos_Totales"], color="#1f77b4")
    ax.set_title("Ingresos totales por ciudad")
    ax.set_ylabel("Ingresos (USD)")
    for i, v in enumerate(p_city["Ingresos_Totales"]):
        ax.text(i, v, f"{v:,.0f}", ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(os.path.join(DIR_SALIDA, "1_ingresos_por_ciudad.png"), dpi=120)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.barh(p_prod["product_category"], p_prod["Venta_Promedio"], color="#2ca02c")
    ax.invert_yaxis()
    ax.set_title("Venta promedio por categoría de producto")
    ax.set_xlabel("Venta promedio (USD)")
    fig.tight_layout()
    fig.savefig(os.path.join(DIR_SALIDA, "2_venta_promedio_por_categoria.png"), dpi=120)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(p_cust["segmento"], p_cust["Ticket_Promedio"], color="#ff7f0e")
    ax.set_title("Ticket promedio por tipo de cliente y género")
    ax.set_ylabel("Ticket promedio (USD)")
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")
    fig.tight_layout()
    fig.savefig(os.path.join(DIR_SALIDA, "3_ticket_por_cliente_genero.png"), dpi=120)
    plt.close(fig)

    print(f"Gráficos guardados en: {DIR_SALIDA}")
except ImportError:
    print("matplotlib/pandas no están instalados. Ejecute: pip install matplotlib pandas")

# Resultados agregados también en CSV (útil para el repositorio y el informe)
df_city.toPandas().to_csv(os.path.join(DIR_SALIDA, "ingresos_por_ciudad.csv"), index=False)

spark.stop()
print("--- PROCESAMIENTO EN BATCH COMPLETADO ---")

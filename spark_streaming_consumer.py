"""
Tarea 3 - Big Data (UNAD) - Consumidor Spark Structured Streaming + Kafka

Lee los eventos JSON del topic 'sensor_data' (generados por kafka_producer.py) y realiza:
  1. Parseo del JSON con un esquema explícito y conversión del timestamp.
  2. Análisis en tiempo real por sensor: número de eventos, temperatura/humedad promedio,
     temperatura máxima y alertas (temperatura > 30 °C). Se imprime en consola y se actualiza
     un gráfico PNG en cada micro-batch (visualización).
  3. Análisis por ventana de tiempo (30 s).

Ejecución (primero el productor, luego este script):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 spark_streaming_consumer.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BOOTSTRAP = "localhost:9092"
TOPIC = "sensor_data"
UMBRAL_ALERTA = 30.0                                   # °C
PNG_SALIDA = "/home/vboxuser/streaming_dashboard.png"  # gráfico que se actualiza en cada batch
CHECKPOINT = "/tmp/checkpoints_sensor_data"

# shuffle.partitions=4: por defecto Spark usa 200 particiones en cada agregación, lo que vuelve
# muy lentos los micro-batches en una máquina virtual pequeña con poco volumen de datos.
spark = (
    SparkSession.builder.appName("SparkKafkaStreaming")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Esquema de los mensajes JSON (debe coincidir con kafka_producer.py)
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# ---------------------------------------------------------------------------
# 1. Lectura desde Kafka y parseo
# ---------------------------------------------------------------------------
df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

eventos = (
    df_kafka.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
    .dropna(subset=["sensor_id", "temperature", "humidity", "event_time"])
)

# ---------------------------------------------------------------------------
# 2. Estadísticas acumuladas por sensor (+ visualización en cada micro-batch)
# ---------------------------------------------------------------------------
por_sensor = eventos.groupBy("sensor_id").agg(
    F.count("*").alias("eventos"),
    F.round(F.avg("temperature"), 2).alias("temp_prom"),
    F.round(F.max("temperature"), 2).alias("temp_max"),
    F.round(F.avg("humidity"), 2).alias("hum_prom"),
    F.sum(F.when(F.col("temperature") > UMBRAL_ALERTA, 1).otherwise(0)).alias("alertas"),
)


def procesar_batch(batch_df, batch_id):
    """Se ejecuta en cada micro-batch: imprime la tabla y regenera el gráfico."""
    if batch_df.rdd.isEmpty():
        return
    resumen = batch_df.orderBy("sensor_id")
    print(f"\n===== ESTADÍSTICAS POR SENSOR - batch {batch_id} =====")
    resumen.show(truncate=False)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        p = resumen.toPandas()  # solo unas pocas filas (un registro por sensor)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))
        ax1.bar(p["sensor_id"], p["temp_prom"], color="#d62728")
        ax1.axhline(UMBRAL_ALERTA, color="gray", linestyle="--", label=f"Umbral {UMBRAL_ALERTA} °C")
        ax1.set_title("Temperatura promedio por sensor")
        ax1.set_ylabel("°C")
        ax1.legend()
        ax2.bar(p["sensor_id"], p["eventos"], color="#1f77b4")
        ax2.set_title("Eventos recibidos por sensor")
        for ax in (ax1, ax2):
            plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
        fig.suptitle(f"Tiempo real (Kafka + Spark Streaming) - batch {batch_id}")
        fig.tight_layout()
        os.makedirs(os.path.dirname(PNG_SALIDA), exist_ok=True)
        fig.savefig(PNG_SALIDA, dpi=110)
        plt.close(fig)
    except ImportError:
        print("matplotlib/pandas no instalados: pip install matplotlib pandas")


q_sensor = (
    por_sensor.writeStream.outputMode("complete")
    .foreachBatch(procesar_batch)
    .option("checkpointLocation", CHECKPOINT + "/por_sensor")
    .start()
)

# ---------------------------------------------------------------------------
# 3. Estadísticas por ventana de tiempo de 30 s (modo complete: se conserva todo el historial)
# ---------------------------------------------------------------------------
por_ventana = (
    eventos.groupBy(F.window("event_time", "30 seconds"))
    .agg(
        F.count("*").alias("eventos"),
        F.round(F.avg("temperature"), 2).alias("temp_prom"),
        F.round(F.avg("humidity"), 2).alias("hum_prom"),
    )
    .orderBy("window")  # ordenar solo es válido en modo complete
)

q_ventana = (
    por_ventana.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT + "/por_ventana")
    .start()
)

spark.streams.awaitAnyTermination()

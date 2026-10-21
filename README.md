# Tarea 3 – Procesamiento de datos con Apache Spark y Kafka (UNAD – Big Data)

Autor: Luis Eduardo Ropero Rivera · Grupo 202016911_47

## Contenido
| Archivo | Descripción |
|---|---|
| `batch_processing.py` | Procesamiento **batch** con PySpark: carga del CSV, limpieza, transformación, EDA (DataFrames + ejemplo RDD) y gráficos PNG |
| `kafka_producer.py` | Productor Kafka: simula sensores y publica JSON en el topic `sensor_data` (1 evento/segundo) |
| `spark_streaming_consumer.py` | Consumidor **Spark Structured Streaming**: estadísticas por sensor y por ventana de 30 s + gráfico PNG por micro-batch |

## Requisitos
Ubuntu 22.04, Java 11/17, Apache Spark 3.5.4, Apache Kafka 3.9.0 (con ZooKeeper), Python 3.
`pip install kafka-python matplotlib pandas`

## Ejecución – Batch
```bash
/opt/Spark/bin/spark-submit batch_processing.py /home/vboxuser/sales.csv /home/vboxuser/salida_batch
```
Los gráficos y el CSV de resultados quedan en `salida_batch/`.

## Ejecución – Tiempo real (cada comando en una terminal distinta)
```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
python3 kafka_producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 spark_streaming_consumer.py
```
El gráfico en vivo se guarda en `/home/vboxuser/streaming_dashboard.png`. Spark UI: `http://IP_VM:4040`.

## Dataset
(Indicar aquí el origen real del `sales.csv`, su tamaño y sus columnas.)

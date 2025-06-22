from kafka import KafkaProducer, KafkaConsumer
from data_generator import RandomDataGenerator
import json
from prometheus_client import start_http_server, Counter, Histogram, Gauge, Summary
import time

# Métricas Prometheus ampliadas
MESSAGES_SENT = Counter('kafka_messages_sent_total', 'Total mensajes enviados a Kafka')
SEND_ERRORS = Counter('kafka_send_errors_total', 'Errores al enviar mensajes a Kafka')
GEN_TIME = Histogram('kafka_message_generation_seconds', 'Tiempo en generar un mensaje aleatorio')
SEND_TIME = Histogram('kafka_message_send_seconds', 'Tiempo en enviar un mensaje a Kafka')
MESSAGE_SIZE = Histogram('kafka_message_size_bytes', 'Tamaño de los mensajes enviados a Kafka')
MESSAGES_IN_PROGRESS = Gauge('kafka_messages_in_progress', 'Mensajes en proceso de envío')
LAST_SEND_STATUS = Gauge('kafka_last_send_status', 'Último estado de envío (1=OK, 0=Error)')
SEND_SUMMARY = Summary('kafka_send_summary_seconds', 'Resumen de tiempos de envío')
SERIALIZE_TIME = Histogram('kafka_message_serialize_seconds', 'Tiempo en serializar un mensaje a JSON')

# Inicia el endpoint Prometheus en el puerto 9101
start_http_server(9101)

def make_producer(host):
    producer = KafkaProducer(bootstrap_servers=[host], value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    return producer

def send_random(producer):
    for msg in RandomDataGenerator():
        with GEN_TIME.time():
            try:
                with SERIALIZE_TIME.time():
                    msg_bytes = json.dumps(msg).encode("utf-8")
                MESSAGE_SIZE.observe(len(msg_bytes))
                MESSAGES_IN_PROGRESS.inc()
                with SEND_TIME.time(), SEND_SUMMARY.time():
                    producer.send("probando", msg)
                MESSAGES_SENT.inc()
                LAST_SEND_STATUS.set(1)
                print("Sent")
            except Exception as e:
                SEND_ERRORS.inc()
                LAST_SEND_STATUS.set(0)
                print(f"Error enviando mensaje: {e}")
            finally:
                MESSAGES_IN_PROGRESS.dec()

def main():
    producer = make_producer(host="kafka:9092")
    send_random(producer)

if __name__ == "__main__":
    main()
from kafka import KafkaConsumer
import json
from storage_mongo import guardar_en_mongo
from prometheus_client import start_http_server, Counter, Histogram, Gauge, Summary

CONSUMED_MESSAGES = Counter('kafka_messages_consumed_total', 'Total mensajes consumidos de Kafka')
CONSUME_ERRORS = Counter('kafka_consume_errors_total', 'Errores al consumir mensajes de Kafka')
CONSUME_TIME = Histogram('kafka_message_consume_seconds', 'Tiempo en procesar un mensaje de Kafka')
CONSUME_IN_PROGRESS = Gauge('kafka_consume_in_progress', 'Mensajes en proceso de consumo')
CONSUME_SUMMARY = Summary('kafka_consume_summary_seconds', 'Resumen de tiempos de consumo')

start_http_server(9102)  # Puerto Prometheus para el consumidor

def main():
    consumer = KafkaConsumer(
        'probando', # 'probando' es el nombre del topic a consumir
        bootstrap_servers='kafka:9092', #Dirección del servidor Kafka, en mi docker es kafka:9092
        auto_offset_reset='earliest', # Para leer desde el principio del topic (si nunca ha leido), 'latest' para leer solo nuevos mensajes
        enable_auto_commit=True, # Kafka guarda el "offset" automáticamente. El offset es un marcador que dice: "ya leí hasta aquí", se guardará automáticamente qué mensajes ya leí (en el grupo de consumidores)
        group_id='hrpro-consumer-group', # Identificador del grupo de consumidores, si no se especifica, se crea uno por defecto
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializador para convertir el mensaje de bytes a JSON
    )

    #print("Esperando mensajes...")
    for message in consumer:
        #print("Mensaje recibido:")
        #print(json.dumps(message.value, indent=2))  # bonito para debug

        with CONSUME_TIME.time(), CONSUME_SUMMARY.time():
            try:
                CONSUME_IN_PROGRESS.inc()
                guardar_en_mongo(message.value)
                CONSUMED_MESSAGES.inc()
            except Exception as e:
                CONSUME_ERRORS.inc()
            finally:
                CONSUME_IN_PROGRESS.dec()


            

if __name__ == "__main__":
    #print("|||- Iniciando consumidor de Kafka...")
    main()

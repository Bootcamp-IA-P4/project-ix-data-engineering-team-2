from kafka import KafkaConsumer
import json
import redis
import hashlib
from storage_mongo import guardar_en_mongo, conectar_mongo
from prometheus_client import start_http_server, Counter, Histogram, Gauge, Summary

CONSUMED_MESSAGES = Counter('kafka_messages_consumed_total', 'Total mensajes consumidos de Kafka')
CONSUME_ERRORS = Counter('kafka_consume_errors_total', 'Errores al consumir mensajes de Kafka')
CONSUME_TIME = Histogram('kafka_message_consume_seconds', 'Tiempo en procesar un mensaje de Kafka')
CONSUME_IN_PROGRESS = Gauge('kafka_consume_in_progress', 'Mensajes en proceso de consumo')
CONSUME_SUMMARY = Summary('kafka_consume_summary_seconds', 'Resumen de tiempos de consumo')

start_http_server(9102)  # Puerto Prometheus para el consumidor
# Resto de tus imports
from etl.utils.logg import write_log

# Configurar Redis (host y puerto pueden venir de variables de entorno si quieres)
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

def fingerprint(message):
    # Creamos hash único para mensaje para deduplicar
    return hashlib.sha256(json.dumps(message, sort_keys=True).encode('utf-8')).hexdigest()

def main():
    conectar_mongo()
    try:
        consumer = KafkaConsumer(
            'probando', # 'probando' es el nombre del topic a consumir
            bootstrap_servers='kafka:9092', #Dirección del servidor Kafka, en mi docker es kafka:9092
            auto_offset_reset='earliest', # Para leer desde el principio del topic (si nunca ha leido), 'latest' para leer solo nuevos mensajes
            enable_auto_commit=True, # Kafka guarda el “offset” automáticamente. El offset es un marcador que dice: “ya leí hasta aquí”, se guardará automáticamente qué mensajes ya leí (en el grupo de consumidores)
            group_id='hrpro-consumer-group', # Identificador del grupo de consumidores, si no se especifica, se crea uno por defecto
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializador para convertir el mensaje de bytes a JSON
        )
        write_log("INFO", "consumer.py", "Kafka consumer iniciado y conectado.")
    except Exception as e:
        print(f"Error iniciando Kafka consumer: {e}")
        write_log("ERROR", "consumer.py", f"Error iniciando Kafka consumer: {e}")
        return

    for message in consumer:
        # # --- DEBUG: simular mensaje duplicado ---
        # test_msg = {"fullname": "Ana García", "city": "Madrid"}
        # fp = fingerprint(test_msg)

        # if not redis_client.exists(fp):
        #     print("🔄 Primer mensaje (nuevo): lo procesamos")
        #     redis_client.set(fp, 1, ex=86400)
        # else:
        #     print("❌ Mensaje duplicado: ignorado")
        msg = message.value
        fp = fingerprint(msg)

        # Deduplicar usando Redis
        if redis_client.exists(fp):
            write_log("INFO", "consumer.py", f"Mensaje duplicado ignorado: {fp}")
            continue

        with CONSUME_TIME.time(), CONSUME_SUMMARY.time():
            try:
                CONSUME_IN_PROGRESS.inc()
                guardar_en_mongo(message.value)
                CONSUMED_MESSAGES.inc()
            except Exception as e:
                write_log("ERROR", "consumer.py", f"Error procesando mensaje: {e}")
                print(f"Error procesando mensaje: {e}")
                CONSUME_ERRORS.inc()
            finally:
                CONSUME_IN_PROGRESS.dec()

            # Guardar en MongoDB
            guardar_en_mongo(msg)

            # Guardar fingerprint en Redis con TTL 1 día (86400 segundos)
            redis_client.set(fp, 1, ex=86400)

    write_log("INFO", "consumer.py", f"Mensaje procesado y guardado: {fp}")

if __name__ == "__main__":
    main()
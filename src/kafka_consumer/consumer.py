from kafka import KafkaConsumer
import json
import redis
import hashlib
from storage_mongo import guardar_en_mongo
import sys
import os

# Resto de tus imports
from etl.utils.logg import write_log
from etl.etl_utils import clean_data, register_keys_redis,find_person_key_redis


# Configurar Redis (host y puerto pueden venir de variables de entorno si quieres)
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

def fingerprint(message):
    # Creamos hash único para mensaje para deduplicar
    return hashlib.sha256(json.dumps(message, sort_keys=True).encode('utf-8')).hexdigest()


# def write_log(level, module, message):
#     print(f"{level} - {module} - {message}")

def main():
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
        write_log("ERROR", "consumer.py", f"Error iniciando Kafka consumer: {e}")
        return

    for message in consumer:
        try:
            msg = message.value
            fp = fingerprint(msg)

            if redis_client.exists(fp):
                write_log("INFO", "consumer.py", f"Mensaje duplicado ignorado: {fp}")
                continue

            guardar_en_mongo(msg)
            msg = clean_data(msg)  # Limpieza de campos
            existing_idx = find_person_key_redis(msg)  #busca si alguna clave de las q vienen (identifying keys) ya tiene asociado algun indice
            if existing_idx is not None:
                idx = existing_idx
            else:
                idx = redis_client.incr('global_person_idx') - 1

            register_keys_redis(msg, idx) # asigna todas las claves al indice ya registrado (p.ej. nº SS: 5897; pasp:123 se va a asociar a indice 12 si el pasp:123 estaba asociado al ind 12)

            
            redis_client.set(fp, 1, ex=86400)
            write_log("INFO", "consumer.py", f"Mensaje procesado y guardado: {fp}")

        except Exception as e:
            write_log("ERROR", "consumer.py", f"Error procesando mensaje: {e}")


if __name__ == "__main__":
    #print("|||- Iniciando consumidor de Kafka...")
    main()

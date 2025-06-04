from kafka import KafkaConsumer
import json
from aggregator import agregar_datos
from storage_mongo import guardar_en_mongo

def main():
    consumer = KafkaConsumer(
        'probando', # 'probando' es el nombre del topic a consumir
        bootstrap_servers='kafka:9092', #Dirección del servidor Kafka, en mi docker es kafka:9092
        auto_offset_reset='earliest', # Para leer desde el principio del topic (si nunca ha leido), 'latest' para leer solo nuevos mensajes
        enable_auto_commit=True, # Kafka guarda el “offset” automáticamente. El offset es un marcador que dice: “ya leí hasta aquí”, se guardará automáticamente qué mensajes ya leí (en el grupo de consumidores)
        group_id='hrpro-consumer-group', # Identificador del grupo de consumidores, si no se especifica, se crea uno por defecto
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializador para convertir el mensaje de bytes a JSON
    )

    print("Esperando mensajes...")
    for message in consumer:
        print("Mensaje recibido:")
        print(json.dumps(message.value, indent=2))  # bonito para debug

        # Guardar en Mongo
        guardar_en_mongo(message.value)

        # Agregar datos y verificar si se puede unificar
        print("Agregando datos...")
        resultado = agregar_datos(message.value)
        if resultado:
            print("\n✅ Registro unificado listo para guardar:")
            print(json.dumps(resultado, indent=2))

            

if __name__ == "__main__":
    main()

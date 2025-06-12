from kafka import KafkaConsumer
import json


def main():
    consumer = KafkaConsumer(
        'probando', # 'probando' es el nombre del topic a consumir
        bootstrap_servers='kafka:9092', #Dirección del servidor Kafka, en mi docker es kafka:9092
        auto_offset_reset='earliest', # Para leer desde el principio del topic (si nunca ha leido), 'latest' para leer solo nuevos mensajes
        group_id='hrpro-consumer-group', # Identificador del grupo de consumidores, si no se especifica, se crea uno por defecto
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializador para convertir el mensaje de bytes a JSON
    )

    print("Esperando mensajes...")
    for message in consumer:
        print("Mensaje recibido:")
        print(json.dumps(message.value, indent=2))  # bonito para debug

        

def restar():
       save = KafkaConsumer(    

    enable_auto_commit=True, # Kafka guarda el “offset” automáticamente. El offset es un marcador que dice: “ya leí hasta aquí”, se guardará automáticamente qué mensajes ya leí (en el grupo de consumidores)
       )

import json
from collections import defaultdict

def process_kafka_messages(messages):
    people_data = defaultdict(lambda: {
        'Personal': {},
        'Location': {},
        'Professional': {},
        'Bank': {},
        'Net': {}
    })
    
    for message in messages:
        try:
            # Extraer el valor del mensaje (asumiendo formato JSON)
            value = json.loads(message.split('Value: ')[1])
            
            # Identificar si hay un nombre completo
            fullname = value.get('fullname')
            if not fullname:
                # Si no hay fullname, buscar en campos de nombre/apellido
                name = value.get('name', '')
                last_name = value.get('last_name', '')
                fullname = f"{name} {last_name}".strip()
                if not fullname:
                    continue  # No podemos agrupar sin identificador
                
            # Determinar el tipo de datos y agrupar
            if 'passport' in value or 'IBAN' in value or 'salary' in value:
                people_data[fullname]['Bank'].update(value)
            elif 'company' in value or 'job' in value:
                people_data[fullname]['Professional'].update(value)
            elif 'address' in value or 'city' in value:
                people_data[fullname]['Location'].update(value)
            elif 'IPv4' in value:
                people_data[fullname]['Net'].update(value)
            else:
                # Datos personales (nombre, teléfono, email, etc.)
                people_data[fullname]['Personal'].update(value)
                
        except (IndexError, json.JSONDecodeError) as e:
            print(f"Error procesando mensaje: {message}. Error: {e}")
    
    return people_data

# Ejemplo de uso (necesitarías extraer los mensajes del archivo primero)
# messages = [line.split('Value: ')[1] for line in open('mensajes_kafka.txt') if 'Value: ' in line]
# processed_data = process_kafka_messages(messages)
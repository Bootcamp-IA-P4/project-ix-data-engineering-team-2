from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'hr_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

people_buffer = {}

for msg in consumer:
    mensaje = msg.value
    persona_completa = process_kafka_message(mensaje, people_buffer)
    
    if persona_completa:
        # Guardar en MongoDB o SQL
        guardar_en_base_de_datos(persona_completa)

        # Limpiar buffer
        identifier = extract_identifier(mensaje['type'], mensaje['data'])
        del people_buffer[identifier]

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import json

BOOTSTRAP_SERVERS = ['kafka:9092']

def main():
    consumer = KafkaConsumer(
        'probando',
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='etl_group',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    try:
        print("Listando mensajes...")
        for msg in consumer:
            data = msg.value
            print(f"Mensaje recibido: {data}")
    except KeyboardInterrupt:
        print("El consumer esta parado.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

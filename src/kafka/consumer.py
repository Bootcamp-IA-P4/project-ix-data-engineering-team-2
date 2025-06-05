from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import json
from kafka.errors import NoBrokersAvailable
import threading



class KafkaConsumerClient:
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.connected = False

    def connect(self):
        try:
            self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            print(f"Topics disponibles: {self.admin_client.list_topics()}")
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            self.connected = True
            print("Conexión exitosa con Kafka.")
        except NoBrokersAvailable:
            print("No se pudo conectar con el servidor de Kafka.")
            self.connected = False

    def is_connected(self):
        return self.connected

    def collect_messages(self, output_file='mensajes_kafka.txt'):
        def run():
            if not self.connected or self.consumer is None:
                print("No conectado a Kafka. No se pueden recolectar mensajes.")
                return
            try:
                with open(output_file, 'a', encoding='utf-8') as f:
                    print(f"Escuchando topic: {self.topic}")
                    for msg in self.consumer:
                        linea = f"[{msg.topic}] Offset: {msg.offset} | Timestamp: {msg.timestamp} | Partition: {msg.partition} | Value: {msg.value}\n"
                        print(linea.strip())
                        f.write(linea)
                        self.consumer.commit()
            except KeyboardInterrupt:
                print("Consumo interrumpido por el usuario.")
            finally:
                self.consumer.close()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        print("El consumo de mensajes se está ejecutando en un segundo hilo.")
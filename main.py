from fastapi import FastAPI
from kafka  import KafkaConsumer, TopicPartition
from src.kafka.consumer import KafkaConsumerClient
from utils.core import settings
from contextlib import asynccontextmanager
import asyncio
import time

BOOTSTRAP_SERVERS = f"{settings.kafka_host}:{settings.bootstrap_port}"
TOPIC = 'mi-topico'
PARTITION = 0
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Cargar el consumidor de Kafka al iniciar la aplicación de forma asincrona
    def sync_load():
        print("Cargando consumidor de Kafka...")
        
        consumer_client = KafkaConsumerClient(BOOTSTRAP_SERVERS, settings.topic, settings.group_id)
        try:
            consumer_client.connect()
            if consumer_client.is_connected():
                print("Cargando consumidor de Kafka...")
                consumer_client.collect_messages()
            else:
                print("No se pudo conectar al consumidor de Kafka.")
        except Exception as e:
            print(f"Error al cargar el consumidor de Kafka: {e}.")
    asyncio.get_event_loop().run_in_executor(None, sync_load)
    yield

#Crear la app
app = FastAPI(
    lifespan=lifespan,
    title=settings.proyect_name,
    description=settings.description,
    version=settings.version,
    )
@app.get("/")
async def read_root():
    return {
        "message": "Kafka Consumer is running",
        "Team   ": settings.creators,
        }

@app.get("/kafka/lag")
def get_kafka_lag():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=settings.group_id,
            enable_auto_commit=False
        )
        
        tp = TopicPartition(TOPIC, PARTITION)
        consumer.assign([tp])

        # Obtiene el último offset leído por el consumidor
        current_offset = consumer.position(tp)

        # Obtiene el último offset producido
        end_offset = consumer.end_offsets([tp])[tp]

        lag = end_offset - current_offset

        return {
            "topic": TOPIC,
            "partition": PARTITION,
            "current_offset": current_offset,
            "end_offset": end_offset,
            "lag": lag
        }

    except Exception as e:
        return {"error": str(e)}
    

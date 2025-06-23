from etl_utils import clean_data, find_person_key, register_keys, people_data, register_keys_redis, save_batch
from etl_utils import create_kafka_consumer  # Si tienes esta función activa

def run_kafka_consumer():
    print("⏳ Escuchando mensajes de Kafka para prueba... Ctrl+C para detener.")
    message_count = 0
    batch_number = 1
    SAVE_INTERVAL = 5000

    try:
        consumer = create_kafka_consumer()
        for message in consumer:
            record = message.value
            record = clean_data(record)
            idx = find_person_key(record)
            if idx is not None:
                people_data[idx].update(record)
            else:
                idx = len(people_data)
                people_data.append(record)
            register_keys(record, idx)
            register_keys_redis(record, idx)

            message_count += 1
            if message_count % SAVE_INTERVAL == 0:
                save_batch(batch_number)
                batch_number += 1

    except KeyboardInterrupt:
        print("\n🛑 Interrumpido por el usuario.")

    finally:
        save_batch(batch_number, final=True)

if __name__ == "__main__":
    run_kafka_consumer()

from kafka import KafkaConsumer
import json
import csv
import os

# --- Configuración de Kafka ---
KAFKA_TOPIC = "probando"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# --- Estructuras para datos y asociación ---
people_data = []
person_index = {}
identifying_keys = ['passport', 'email', 'fullname', 'telfnumber', 'name', 'last_name']

# --- Claves por categoría ---
grouping_map = {
    'personal_': ['name', 'last_name', 'sex', 'telfnumber', 'passport', 'email'],
    'location_': ['address', 'city', 'IPv4'],
    'professional_': ['fullname', 'company', 'company address', 'company_telfnumber', 'company_email', 'job'],
    'bank_': ['IBAN', 'salary']
}

def find_person_key(record):
    for key in identifying_keys:
        if key in record:
            val = record[key]
            if val in person_index:
                return person_index[val]
    return None

def register_keys(record, idx):
    for key in identifying_keys:
        if key in record:
            person_index[record[key]] = idx

def flatten_with_prefix(record):
    """Devuelve una versión aplanada con prefijos según categoría"""
    flat = {}
    for prefix, keys in grouping_map.items():
        for key in keys:
            if key in record:
                flat[prefix + key] = record[key]
    return flat

def save_batch(batch_number, final=False):
    # JSON
    filename_json = f"personas_combinadas_batch_{batch_number}.json" if not final else "personas_combinadas_final.json"
    with open(filename_json, "w", encoding="utf-8") as fjson:
        json.dump(people_data, fjson, indent=2, ensure_ascii=False)
    print(f"💾 JSON guardado: {filename_json}")

    # CSV
    filename_csv = f"personas_combinadas_batch_{batch_number}.csv" if not final else "personas_combinadas_final.csv"
    flat_people = [flatten_with_prefix(p) for p in people_data]

    # Obtener todas las columnas posibles
    all_keys = set()
    for person in flat_people:
        all_keys.update(person.keys())
    all_keys = sorted(all_keys)

    with open(filename_csv, "w", encoding="utf-8", newline='') as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=all_keys)
        writer.writeheader()
        for person in flat_people:
            writer.writerow(person)
    print(f"📄 CSV guardado: {filename_csv}")

# --- Escucha de mensajes ---
print("⏳ Escuchando mensajes de Kafka... Ctrl+C para detener.")
message_count = 0
batch_number = 1
SAVE_INTERVAL = 5000

try:
    for message in consumer:
        record = message.value
        idx = find_person_key(record)
        if idx is not None:
            people_data[idx].update(record)
        else:
            idx = len(people_data)
            people_data.append(record)
        register_keys(record, idx)

        message_count += 1
        if message_count % SAVE_INTERVAL == 0:
            save_batch(batch_number)
            batch_number += 1

except KeyboardInterrupt:
    print("\n🛑 Interrumpido por el usuario.")

finally:
    save_batch(batch_number, final=True)
    print("✅ Todos los datos consolidados y guardados.")


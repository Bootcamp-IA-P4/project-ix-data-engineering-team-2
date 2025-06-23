from kafka import KafkaConsumer
import json
import csv
from etl.utils.logg import write_log
import redis

# --- Configuración de Kafka ---
KAFKA_TOPIC = "probando" 
KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
# KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]


# Configuración del cliente Redis
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# --- Estructuras para datos y asociación ---
people_data = []
person_index = {}
identifying_keys = ['passport', 'email', 'fullname', 'telfnumber', 'name', 'last_name', 'address']

# --- Claves por categoría ---
grouping_map = {
    'personal_': ['name', 'last_name', 'sex', 'telfnumber', 'passport', 'email'],
    'location_': ['fullname', 'address', 'city', 'IPv4'],
    'professional_': ['fullname', 'company', 'company address', 'company_telfnumber', 'company_email', 'job'],
    'bank_': ['passport', 'IBAN', 'salary']
}
import re

TITLES = ['mr', 'mrs', 'ms', 'sr', 'sra', 'dr', 'dott', 'sig', 'mtro', 'sr(a)', 'mrs.', 'dr.', 'ms.', 'sr.', 'sig.']

def clean_name(name):
    if not isinstance(name, str):
        return name
    name_clean = name.lower()
    for title in TITLES:
        name_clean = re.sub(rf'\b{re.escape(title)}\b', '', name_clean, flags=re.IGNORECASE)
    name_clean = re.sub(r'[().]', '', name_clean)
    return name_clean.strip().title()

def clean_telf(telf):
    if not isinstance(telf, str):
        return telf
    return re.sub(r'[^+0-9]', '', telf)

def clean_address_field(text):
    if not isinstance(text, str):
        return text
    return text.replace(',', '')

def clean_data(record):
    write_log("INFO", "etl_utils.py", f"Procesando registro: {record}")
    if 'name' in record:
        record['name'] = clean_name(record['name'])

    if 'telfnumber' in record:
        record['telfnumber'] = clean_telf(record['telfnumber'])

    if 'company_telfnumber' in record:
        record['company_telfnumber'] = clean_telf(record['company_telfnumber'])

    if 'city' in record:
        record['city'] = clean_address_field(record['city'])

    if 'address' in record:
        record['address'] = clean_address_field(record['address'])

    if 'company address' in record:
        record['company address'] = clean_address_field(record['company address'])

    if 'sex' in record:
        value = record['sex']
        if isinstance(value, list) and len(value) > 0:
            record['sex'] = str(value[0])
        elif isinstance(value, str):
            record['sex'] = value

    return record

# Busca una clave de persona en Redis y devuelve su índice
def find_person_key_redis(record):
    for key in identifying_keys:
        if key in record:
            val = record[key]
            idx = redis_client.get(f"person_key:{val}")
            if idx is not None:
                return int(idx)
    return None

# Registra las claves en Redis para poder encontrarlas rápidamente
def register_keys_redis(record, idx):
    for key in identifying_keys:
        if key in record:
            redis_client.set(f"person_key:{record[key]}", idx)

def find_person_key(record):
    for key in identifying_keys:
        if key in record:
            val = record[key]
            if val in person_index:
                return person_index[val]
    # Relaciona por fullname si existe
    if "fullname" in record:
        val = record["fullname"]
        if val in person_index:
            return person_index[val]
    # Relaciona por address si existe
    if "address" in record:
        val = record["address"]
        if val in person_index:
            return person_index[val]
    return None

def find_all_person_indices(record):
    indices = set()
    for key in identifying_keys:
        if key in record and record[key] in person_index:
            indices.add(person_index[record[key]])
            idx = redis_client.get(f"person_key:{record[key]}") # Busca en Redis
            if idx is not None:
                indices.add(int(idx)) 
    return list(indices)


def find_indices_redis(record): 
    indices = set()
    for key in identifying_keys:
        if key in record:
            idx = redis_client.get(f"person_key:{record[key]}")
            if idx is not None:
                indices.add(int(idx))
    return list(indices)

def merge_people(indices):
    if not indices:
        return None
    indices = sorted(indices)
    base_idx = indices[0]
    # Fusiona todos los datos en base_idx
    for idx in indices[1:]:
        people_data[base_idx].update(people_data[idx])
    # Elimina duplicados de mayor a menor para no desordenar la lista
    for idx in sorted(indices[1:], reverse=True):
        del people_data[idx]
    redis_client.flushdb()  # Limpiar Redis para evitar inconsistencias
    # Reconstruye person_index para que apunten a los nuevos índices
    person_index.clear()
    for idx, person in enumerate(people_data):
        register_keys_redis(person, idx) 
        for key in identifying_keys:
            if key in person:
                person_index[person[key]] = idx
    return base_idx

def register_keys(record, idx):
    for key in identifying_keys:
        if key in record:
            person_index[record[key]] = idx

def group_records(records):
    for record in records:
        indices = find_all_person_indices(record)
        #indices = find_indices_redis(record)
        if indices:
            base_idx = merge_people(indices)
            people_data[base_idx].update(record)
            register_keys(people_data[base_idx], base_idx)
            register_keys_redis(people_data[base_idx], base_idx)
        else:
            idx = len(people_data)
            people_data.append(record)
            register_keys(record, idx)
            register_keys_redis(record, idx)

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
    try:
        with open(filename_json, "w", encoding="utf-8") as fjson:
            json.dump(people_data, fjson, indent=2, ensure_ascii=False)
        write_log("INFO", "etl_utils.py", f"JSON guardado: {filename_json}")
    except Exception as e:
        write_log("ERROR", "etl_utils.py", f"Error al guardar JSON {filename_json}: {str(e)}")

    # CSV
    filename_csv = f"personas_combinadas_batch_{batch_number}.csv" if not final else "personas_combinadas_final.csv"
    try:
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
        write_log("INFO", "etl_utils.py", f"CSV guardado: {filename_csv}")
    except Exception as e:
        write_log("ERROR", "etl_utils.py", f"Error al guardar CSV {filename_csv}: {str(e)}")

# def create_kafka_consumer():
#     try:
#         consumer = KafkaConsumer(
#             KAFKA_TOPIC,
#             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#             auto_offset_reset="earliest",
#             enable_auto_commit=True,
#             value_deserializer=lambda x: json.loads(x.decode("utf-8"))
#         )
#         write_log("INFO", "etl_utils.py", "Kafka consumer creado exitosamente")
#         return consumer
#     except Exception as e:
#         write_log("ERROR", "etl_utils.py", f"Error al crear Kafka consumer: {str(e)}")
#         raise

# --- Escucha de mensajes ---
#print("⏳ Escuchando mensajes de Kafka... Ctrl+C para detener.")
# message_count = 0
# batch_number = 1
# SAVE_INTERVAL = 5000

# try:
#     for message in create_kafka_consumer():
#         record = message.value
#         record = clean_data(record)
#         idx = find_person_key(record)
#         if idx is not None:
#             people_data[idx].update(record)
#         else:
#             idx = len(people_data)
#             people_data.append(record)
#         register_keys(record, idx)

#         message_count += 1
#         if message_count % SAVE_INTERVAL == 0:
#             save_batch(batch_number)
#             batch_number += 1

# except KeyboardInterrupt:
#     print("\n🛑 Interrumpido por el usuario.")

# finally:
#     save_batch(batch_number, final=True)
#     #print("✅ Todos los datos consolidados y guardados.")

def nombres_en_fullname(name, last_name, fullname):
    """Devuelve True si name y last_name están en fullname (ignorando mayúsculas/minúsculas y acentos)."""
    if not (name and last_name and fullname):
        return False
    name = name.lower().strip()
    last_name = last_name.lower().strip()
    fullname = fullname.lower()
    return name in fullname and last_name in fullname

def address_match(addr1, addr2):
    """Devuelve True si las direcciones son iguales ignorando mayúsculas/minúsculas y espacios."""
    if not (addr1 and addr2):
        return False
    return addr1.lower().replace(" ", "") == addr2.lower().replace(" ", "")



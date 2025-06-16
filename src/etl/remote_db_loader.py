import os
import json
from kafka import KafkaConsumer
from supabase import create_client, Client
from etl_utils import clean_data, find_person_key, register_keys, identifying_keys, person_index, people_data

# Configuración de Kafka
KAFKA_TOPIC = "probando"
# KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]

# Configuración de Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def insert_location(address, city):
    try:
        resp = supabase.table("locations").select("location_id").eq("address", address).eq("city", city).execute()
        if resp.data:
            return resp.data[0]["location_id"]
        else:
            ins = supabase.table("locations").insert({"address": address, "city": city}).execute()
            return ins.data[0]["location_id"]
    except Exception as e:
        print(f"❌ Error al insertar en locations ({address}, {city}):", e)
        return None

def insert_person(person, location_id):
    data = {
        "passport": person.get("passport"),
        "first_name": person.get("name"),
        "last_name": person.get("last_name"),
        "sex": person.get("sex"),
        "phone_number": person.get("telfnumber"),
        "email": person.get("email"),
        "location_id_fk": location_id
    }
    try:
        supabase.table("persons").upsert(data, on_conflict=["passport"]).execute()
    except Exception as e:
        print("❌ Error al guardar en Supabase (persons):", e)

def insert_bank(person):
    if "IBAN" not in person:
        return
    data = {
        "passport_fk": person.get("passport"),
        "iban": person.get("IBAN"),
        "salary": person.get("salary")
    }
    try:
        supabase.table("bank_data").upsert(data, on_conflict=["passport_fk"]).execute()
    except Exception as e:
        print("❌ Error al guardar en Supabase (bank_data):", e)

def insert_network(person, location_id):
    if "IPv4" not in person:
        return
    data = {
        "passport_fk": person.get("passport"),
        "location_id_fk": location_id,
        "ip_address": person.get("IPv4")
    }
    try:
        supabase.table("network_data").upsert(data, on_conflict=["passport_fk"]).execute()
    except Exception as e:
        print("❌ Error al guardar en Supabase (network_data):", e)

def insert_professional(person):
    if "company" not in person:
        return
    data = {
        "passport_fk": person.get("passport"),
        "company_name": person.get("company"),
        "company_address": person.get("company address"),
        "company_phone_number": person.get("company_telfnumber"),
        "company_email": person.get("company_email"),
        "job_title": person.get("job")
    }
    try:
        supabase.table("professional_data").upsert(data, on_conflict=["passport_fk"]).execute()
    except Exception as e:
        print("❌ Error al guardar en Supabase (professional_data):", e)

def process_message(message):
    record = clean_data(message)
    idx = find_person_key(record)
    if idx is not None:
        people_data[idx].update(record)
    else:
        idx = len(people_data)
        people_data.append(record)
    register_keys(record, idx)

    person = people_data[idx]
    address = person.get("address")
    city = person.get("city")
    location_id = None
    if address and city:
        print(f"🌍 Insertando ubicación: {address}, {city}")
        location_id = insert_location(address, city)
    insert_person(person, location_id)
    insert_bank(person)
    insert_network(person, location_id)
    insert_professional(person)

def main():
    print("🔗 Conectando a Kafka...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print("⏳ Escuchando mensajes de Kafka y enviando a Supabase...")
    try:
        print("🔄 Iniciando el procesamiento de mensajes...")
        for message in consumer:
            process_message(message.value)
            print("✅✅  Mensaje procesado e insertado en Supabase.")
    except KeyboardInterrupt:
        print("\n🛑 Interrumpido por el usuario.")

if __name__ == "__main__":
    print("🚀 Iniciando el cargador de base de datos remota...")
    main()
import os
import json
from kafka import KafkaConsumer
from supabase import create_client, Client
from src.etl.kafka_to_mongo_etl import clean_data

# Configuración de Kafka
KAFKA_TOPIC = "probando"
KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]  # Cambia si tu broker está en otra dirección

# Configuración de Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def insert_location(address, city):
    resp = supabase.table("locations").select("location_id").eq("address", address).eq("city", city).execute()
    if resp.data:
        return resp.data[0]["location_id"]
    else:
        ins = supabase.table("locations").insert({"address": address, "city": city}).execute()
        return ins.data[0]["location_id"]

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
    supabase.table("persons").upsert(data, on_conflict=["passport"]).execute()

def insert_bank(person):
    if "IBAN" not in person:
        return
    data = {
        "passport_fk": person.get("passport"),
        "iban": person.get("IBAN"),
        "salary": person.get("salary")
    }
    supabase.table("bank_data").upsert(data, on_conflict=["passport_fk"]).execute()

def insert_network(person, location_id):
    if "IPv4" not in person:
        return
    data = {
        "passport_fk": person.get("passport"),
        "location_id_fk": location_id,
        "ip_address": person.get("IPv4")
    }
    supabase.table("network_data").upsert(data, on_conflict=["passport_fk"]).execute()

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
    supabase.table("professional_data").upsert(data, on_conflict=["passport_fk"]).execute()

def process_message(message):
    person = clean_data(message)
    address = person.get("address")
    city = person.get("city")
    location_id = None
    if address and city:
        location_id = insert_location(address, city)
    insert_person(person, location_id)
    insert_bank(person)
    insert_network(person, location_id)
    insert_professional(person)

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print("⏳ Escuchando mensajes de Kafka y enviando a Supabase...")
    try:
        for message in consumer:
            process_message(message.value)
            print("✅ Mensaje procesado e insertado.")
    except KeyboardInterrupt:
        print("\n🛑 Interrumpido por el usuario.")

if __name__ == "__main__":
    main()
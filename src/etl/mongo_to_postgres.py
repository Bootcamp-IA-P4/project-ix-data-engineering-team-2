import os
from dotenv import load_dotenv
from pymongo import MongoClient
from supabase import create_client
from etl_utils import (
    clean_data,
    find_person_key,
    register_keys,
    identifying_keys,
    person_index,
    people_data,
)
from utils.logg import write_log


# Cargar variables de entorno
load_dotenv()

# Configuración de MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:adminpassword@mongo:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'raw_mongo_db')  # Cambiado a raw_mongo_db que es el que usas

# Configuración de Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

def check_mongo_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[MONGO_DB]
        doc = db.personal_data.find_one()
        if doc:
            print(f"✅ Conexión a MongoDB exitosa. Ejemplo de registro extraído:\n{doc}")
            write_log("INFO", "mongo_to_postgres.py", f"Conexión a MongoDB exitosa. Registro: {doc}")
        else:
            print("⚠️ Conexión a MongoDB exitosa, pero no hay registros en personal_data.")
            write_log("INFO", "mongo_to_postgres.py", "Conexión a MongoDB exitosa, pero sin registros.")
        return True
    except Exception as e:
        print(f"❌ Error de conexión a MongoDB: {e}")
        write_log("ERROR", "mongo_to_postgres.py", f"Error de conexión a MongoDB: {e}")
        return False

def check_supabase_connection():
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        resp = supabase.table("locations").select("*").limit(1).execute()
        print("✅ Conexión a Supabase exitosa.")
        write_log("INFO", "mongo_to_postgres.py", "Conexión a Supabase exitosa.")
        return True
    except Exception as e:
        print(f"❌ Error de conexión a Supabase: {e}")
        write_log("ERROR", "mongo_to_postgres.py", f"Error de conexión a Supabase: {e}")
        return False

def get_mongo_collections():
    """Obtiene todas las coleccio                                                                    1.0s
 => [kafka_consumer internal] load .dockerignore                                                                                                            0.0s
 => => transferring context: 2B                                                                                                                             0.0s
 => [kafka_consumer 1/6] FROM docker.io/library/python:3.10-slim@sha256:034724ef64585eeb0e82385e9aabcbeabfe5f7cae2c2dcedb1da95114372b6d7                    0.0s
 => [kafka_consumer internal] load build context                                                                                                            2.9s
 => => transferring context: 1.09MB                                                                                                                         2.9s
 => [random_generator internal] load .dockerignore                                                                                                          0.0s
 => => transferring context: 2B                                                                                                                             0.0s
 => [random_generator internal] load build context                                                                                                          0.0s
 => => transferring context: 171B                                                                                                                           0.0s
 => [random_generator 1/4] FROM docker.io/library/python:3@sha256:5f69d22a88dd4cc4ee1576def19aef48c8faa1b566054c44291183831cbad13b                          0.0s
 => CACHED [random_generator 2/4] WORKDIR /usr/src/app                 nes de MongoDB"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return {
        "personal_data": db["personal_data"],
        "location_data": db["location_data"],
        "professional_data": db["professional_data"],
        "bank_data": db["bank_data"],
        "net_data": db["net_data"]
    }

def extract_and_group_records(collections):
    # Limpiar estructuras globales
    people_data.clear()
    person_index.clear()

    # Extraer y limpiar todos los documentos de todas las colecciones
    all_docs = []
    for cname, col in collections.items():
        for doc in col.find():
            doc.pop("_id", None)
            clean_doc = clean_data(doc)
            all_docs.append(clean_doc)
            write_log("INFO", "mongo_to_postgres.py", f"Documento extraído y limpiado de {cname}: {clean_doc}")

    # Agrupar usando la lógica de etl_utils
    for record in all_docs:
        idx = find_person_key(record)
        if idx is not None:
            people_data[idx].update(record)
        else:
            idx = len(people_data)
            people_data.append(record)
        register_keys(record, idx)
    print(f"✅ Total de personas agrupadas: {len(people_data)}")
    write_log("INFO", "mongo_to_postgres.py", f"Total de personas agrupadas: {len(people_data)}")
    return people_data

def insert_location(supabase, person):
    address = person.get("address")
    city = person.get("city")
    if not address or not city:
        return None
    try:
        resp = supabase.table("locations").select("location_id").eq("address", address).eq("city", city).execute()
        if resp.data:
            location_id = resp.data[0]["location_id"]
        else:
            ins = supabase.table("locations").insert({"address": address, "city": city}).execute()
            location_id = ins.data[0]["location_id"]
        write_log("INFO", "mongo_to_postgres.py", f"Ubicación insertada/obtenida: {address}, {city} -> {location_id}")
        return location_id
    except Exception as e:
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando ubicación: {e}")
        return None

def insert_person(supabase, person, location_id):
    if not person.get("passport") or not person.get("name") or not person.get("last_name") or not person.get("email"):
        write_log("WARNING", "mongo_to_postgres.py", f"Datos insuficientes para persona: {person}")
        return
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
        write_log("INFO", "mongo_to_postgres.py", f"Persona insertada/actualizada: {data}")
    except Exception as e:
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando persona: {e}")

def insert_bank(supabase, person):
    if not person.get("passport") or not person.get("IBAN"):
        return
    data = {
        "passport_fk": person.get("passport"),
        "iban": person.get("IBAN"),
        "salary": person.get("salary")
    }
    try:
        supabase.table("bank_data").upsert(data, on_conflict=["passport_fk"]).execute()
        write_log("INFO", "mongo_to_postgres.py", f"Bank_data insertado/actualizado: {data}")
    except Exception as e:
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando bank_data: {e}")

def insert_network(supabase, person, location_id):
    if not person.get("passport") or not person.get("IPv4"):
        return
    data = {
        "passport_fk": person.get("passport"),
        "location_id_fk": location_id,
        "ip_address": person.get("IPv4")
    }
    try:
        supabase.table("network_data").upsert(data, on_conflict=["passport_fk"]).execute()
        write_log("INFO", "mongo_to_postgres.py", f"Network_data insertado/actualizado: {data}")
    except Exception as e:
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando network_data: {e}")

def insert_professional(supabase, person):
    if not person.get("passport") or not person.get("company"):
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
        write_log("INFO", "mongo_to_postgres.py", f"Professional_data insertado/actualizado: {data}")
    except Exception as e:
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando professional_data: {e}")

def main():
    # Comprobaciones de conexión
    if not check_mongo_connection():
        print("❌ No se pudo conectar a MongoDB. Abortando.")
        return
    if not check_supabase_connection():
        print("❌ No se pudo conectar a Supabase. Abortando.")
        return

    collections = get_mongo_collections()
    personas = extract_and_group_records(collections)

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    for person in personas:
        # 1. Insertar ubicación y obtener location_id
        location_id = insert_location(supabase, person)
        # 2. Insertar persona
        insert_person(supabase, person, location_id)
        # 3. Insertar tablas hijas
        insert_bank(supabase, person)
        insert_network(supabase, person, location_id)
        insert_professional(supabase, person)

    print(f"✅ Migración completada. Total personas procesadas: {len(personas)}")
    write_log("INFO", "mongo_to_postgres.py", f"Migración completada. Total personas procesadas: {len(personas)}")

if __name__ == "__main__":
    main()
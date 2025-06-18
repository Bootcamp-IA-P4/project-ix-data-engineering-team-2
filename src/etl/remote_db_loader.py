import os
import json
import time
import sys
print(">>> Arrancando remote_db_loader.py", file=sys.stderr)
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from supabase import create_client, Client
from etl_utils import (
    clean_data, find_person_key, register_keys, 
    identifying_keys, person_index, people_data,
    create_kafka_consumer
)
from src.utils.logg import write_log

# Forzar la salida de logs
print("="*50, file=sys.stderr)
print("INICIANDO SCRIPT", file=sys.stderr)
print("="*50, file=sys.stderr)

# Cargar variables de entorno
load_dotenv()
write_log("INFO", "remote_db_loader.py", "Variables de entorno cargadas")

# Verificar variables de entorno
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    write_log("ERROR", "remote_db_loader.py", "Faltan variables de entorno requeridas")
    sys.exit(1)

# Crear variable global para supabase
supabase = None

def test_supabase_connection():
    global supabase
    write_log("INFO", "remote_db_loader.py", "Probando conexión a Supabase...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        response = supabase.table("persons").select("count").limit(1).execute()
        write_log("INFO", "remote_db_loader.py", "Conexión a Supabase exitosa")
        return supabase
    except Exception as e:
        write_log("ERROR", "remote_db_loader.py", f"Error al conectar con Supabase: {str(e)}")
        raise

def insert_location(address, city):
    try:
        resp = supabase.table("locations").select("location_id").eq("address", address).eq("city", city).execute()
        if resp.data:
            return resp.data[0]["location_id"]
        else:
            ins = supabase.table("locations").insert({"address": address, "city": city}).execute()
            return ins.data[0]["location_id"]
    except Exception as e:
        write_log("ERROR", "remote_db_loader.py", f"Error al insertar en locations ({address}, {city}): {str(e)}")
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
        write_log("ERROR", "remote_db_loader.py", f"Error al guardar en Supabase (persons): {str(e)}")

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
        write_log("ERROR", "remote_db_loader.py", f"Error al guardar en Supabase (bank_data): {str(e)}")

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
        write_log("ERROR", "remote_db_loader.py", f"Error al guardar en Supabase (network_data): {str(e)}")

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
        write_log("ERROR", "remote_db_loader.py", f"Error al guardar en Supabase (professional_data): {str(e)}")

def is_location_complete(person):
    return bool(person.get("address") and person.get("city"))

def is_person_complete(person):
    required = ["passport", "name", "last_name", "sex", "telfnumber", "email", "location_id_fk"]
    return all(person.get(field) for field in required)

def is_bank_complete(person):
    required = ["passport", "IBAN", "salary"]
    return all(person.get(field) for field in required)

def is_professional_complete(person):
    required = ["passport", "company", "company address", "company_telfnumber", "company_email", "job"]
    return all(person.get(field) for field in required)

def is_network_complete(person):
    required = ["passport", "IPv4", "location_id_fk"]
    return all(person.get(field) for field in required)

def process_message(message):
    write_log("INFO", "remote_db_loader.py", f"Procesando mensaje: {message}")
    try:
        record = clean_data(message)
        idx = find_person_key(record)
        if idx is not None:
            people_data[idx].update(record)
        else:
            idx = len(people_data)
            people_data.append(record)
        register_keys(record, idx)

        person = people_data[idx]

        # --- LOCATIONS ---
        if is_location_complete(person) and not person.get("inserted_location"):
            write_log("INFO", "remote_db_loader.py", f"Insertando ubicación: {person['address']}, {person['city']}")
            location_id = insert_location(person["address"], person["city"])
            if location_id:
                person["location_id_fk"] = location_id
                person["inserted_location"] = True
                write_log("INFO", "remote_db_loader.py", "Ubicación insertada en locations")
            else:
                write_log("ERROR", "remote_db_loader.py", "No se pudo insertar la ubicación")
        elif not is_location_complete(person):
            write_log("INFO", "remote_db_loader.py", "Ubicación incompleta, esperando más datos...")

        # --- PERSONS ---
        if is_person_complete(person) and person.get("inserted_location") and not person.get("inserted_person"):
            insert_person(person, person.get("location_id_fk"))
            person["inserted_person"] = True
            write_log("INFO", "remote_db_loader.py", "Persona insertada en persons")
        elif is_person_complete(person) and not person.get("inserted_location"):
            write_log("INFO", "remote_db_loader.py", "No se puede insertar persona: la ubicación aún no existe")
        elif not is_person_complete(person):
            write_log("INFO", "remote_db_loader.py", "Registro de persona incompleto, esperando más datos...")

        # --- BANK DATA ---
        if is_bank_complete(person) and person.get("inserted_person") and not person.get("inserted_bank"):
            insert_bank(person)
            person["inserted_bank"] = True
            write_log("INFO", "remote_db_loader.py", "Datos bancarios insertados")
        elif is_bank_complete(person) and not person.get("inserted_person"):
            write_log("INFO", "remote_db_loader.py", "No se puede insertar bank_data: la persona aún no existe en persons")
        elif not is_bank_complete(person):
            write_log("INFO", "remote_db_loader.py", "Registro bancario incompleto, esperando más datos...")

        # --- PROFESSIONAL DATA ---
        if is_professional_complete(person) and person.get("inserted_person") and not person.get("inserted_professional"):
            insert_professional(person)
            person["inserted_professional"] = True
            write_log("INFO", "remote_db_loader.py", "Datos profesionales insertados")
        elif is_professional_complete(person) and not person.get("inserted_person"):
            write_log("INFO", "remote_db_loader.py", "No se puede insertar professional_data: la persona aún no existe en persons")
        elif not is_professional_complete(person):
            write_log("INFO", "remote_db_loader.py", "Registro profesional incompleto, esperando más datos...")

        # --- NETWORK DATA ---
        if is_network_complete(person) and person.get("inserted_person") and person.get("inserted_location") and not person.get("inserted_network"):
            insert_network(person, person.get("location_id_fk"))
            person["inserted_network"] = True
            write_log("INFO", "remote_db_loader.py", "Datos de red insertados")
        elif is_network_complete(person) and (not person.get("inserted_person") or not person.get("inserted_location")):
            write_log("INFO", "remote_db_loader.py", "No se puede insertar network_data: falta persona o ubicación")
        elif not is_network_complete(person):
            write_log("INFO", "remote_db_loader.py", "Registro de red incompleto, esperando más datos...")

        write_log("INFO", "remote_db_loader.py", "Proceso de inserción (condicional) terminado")
    except Exception as e:
        write_log("ERROR", "remote_db_loader.py", f"Error procesando mensaje: {str(e)}")
        raise

def main():
    write_log("INFO", "remote_db_loader.py", "Iniciando el cargador de base de datos remota...")
    try:
        write_log("INFO", "remote_db_loader.py", "Script iniciado")
        # Probar conexión a Supabase primero
        supabase = test_supabase_connection()
        
        # Conectar a Kafka
        write_log("INFO", "remote_db_loader.py", "Conectando a Kafka...")
        consumer = create_kafka_consumer()
        write_log("INFO", "remote_db_loader.py", "Conexión a Kafka establecida")
        
        write_log("INFO", "remote_db_loader.py", "Escuchando mensajes...")
        for message in consumer:
            write_log("INFO", "remote_db_loader.py", f"Mensaje recibido: {message.value}")
            process_message(message.value)
            
    except KeyboardInterrupt:
        write_log("INFO", "remote_db_loader.py", "Interrumpido por el usuario.")
    except Exception as e:
        write_log("ERROR", "remote_db_loader.py", f"Error inesperado: {str(e)}")
        write_log("ERROR", "remote_db_loader.py", f"ERROR FATAL: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
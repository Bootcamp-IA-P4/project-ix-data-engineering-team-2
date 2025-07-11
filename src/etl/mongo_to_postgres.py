import os
from dotenv import load_dotenv
from pymongo import MongoClient
from supabase import create_client
from etl_utils import (
    clean_data,
    address_match,
    nombres_en_fullname
)
from utils.logg import write_log
import json
from prometheus_client import start_http_server, Counter, Histogram, Gauge, Summary


# Cargar variables de entorno
load_dotenv()

# Configuración de MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:adminpassword@mongo:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'raw_mongo_db')  # Cambiado a raw_mongo_db que es el que usas

# Configuración de Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

MIGRATED_PERSONS = Counter('supabase_persons_migrated_total', 'Total personas migradas a Supabase')
SUPABASE_INSERT_ERRORS = Counter('supabase_insert_errors_total', 'Errores al insertar en Supabase')
SUPABASE_INSERT_TIME = Histogram('supabase_insert_seconds', 'Tiempo en insertar en Supabase')
SUPABASE_INSERT_IN_PROGRESS = Gauge('supabase_insert_in_progress', 'Inserciones en proceso en Supabase')
SUPABASE_INSERT_SUMMARY = Summary('supabase_insert_summary_seconds', 'Resumen de tiempos de inserción en Supabase')

start_http_server(9103)  # Puerto Prometheus para este servicio

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
    """Obtiene todas las colecciones de MongoDB"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return {
        "personal_data": db["personal_data"],
        "location_data": db["location_data"],
        "professional_data": db["professional_data"],
        "bank_data": db["bank_data"],
        "net_data": db["net_data"]
    }

def insert_location(supabase, person):
    address = person.get("address")
    city = person.get("city")
    if not address or not city:
        return None
    try:
        with SUPABASE_INSERT_TIME.time(), SUPABASE_INSERT_SUMMARY.time():
            SUPABASE_INSERT_IN_PROGRESS.inc()
            resp = supabase.table("locations").select("location_id").eq("address", address).eq("city", city).execute()
            if resp.data:
                location_id = resp.data[0]["location_id"]
            else:
                ins = supabase.table("locations").insert({"address": address, "city": city}).execute()
                location_id = ins.data[0]["location_id"]
        write_log("INFO", "mongo_to_postgres.py", f"Ubicación insertada/obtenida: {address}, {city} -> {location_id}")
        return location_id
    except Exception as e:
        SUPABASE_INSERT_ERRORS.inc()
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando ubicación: {e}")
        return None
    finally:
        SUPABASE_INSERT_IN_PROGRESS.dec()

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
        with SUPABASE_INSERT_TIME.time(), SUPABASE_INSERT_SUMMARY.time():
            SUPABASE_INSERT_IN_PROGRESS.inc()
            supabase.table("persons").upsert(data, on_conflict=["passport"]).execute()
            MIGRATED_PERSONS.inc()
        write_log("INFO", "mongo_to_postgres.py", f"Persona insertada/actualizada: {data}")
    except Exception as e:
        SUPABASE_INSERT_ERRORS.inc()
        print(f"ERROR >> Fallo insertando en persons: {e}")
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando persons: {e}")
    finally:
        SUPABASE_INSERT_IN_PROGRESS.dec()

def insert_bank(supabase, person):
    if not person.get("passport") or not person.get("IBAN"):
        return
    data = {
        "passport_fk": person.get("passport"),
        "iban": person.get("IBAN"),
        "salary": person.get("salary")
    }
    try:
        with SUPABASE_INSERT_TIME.time(), SUPABASE_INSERT_SUMMARY.time():
            SUPABASE_INSERT_IN_PROGRESS.inc()
            supabase.table("bank_data").upsert(data, on_conflict=["passport_fk"]).execute()
        write_log("INFO", "mongo_to_postgres.py", f"Bank_data insertado/actualizado: {data}")
    except Exception as e:
        SUPABASE_INSERT_ERRORS.inc()
        print(f"ERROR >> Fallo insertando en bank_data: {e}")
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando bank_data: {e}")
    finally:
        SUPABASE_INSERT_IN_PROGRESS.dec()

def insert_network(supabase, person, location_id):
    if not person.get("passport") or not person.get("IPv4"):
        return
    data = {
        "passport_fk": person.get("passport"),
        "location_id_fk": location_id,
        "ip_address": person.get("IPv4")
    }
    try:
        with SUPABASE_INSERT_TIME.time(), SUPABASE_INSERT_SUMMARY.time():
            SUPABASE_INSERT_IN_PROGRESS.inc()
            supabase.table("network_data").upsert(data, on_conflict=["passport_fk"]).execute()
        write_log("INFO", "mongo_to_postgres.py", f"Network_data insertado/actualizado: {data}")
    except Exception as e:
        SUPABASE_INSERT_ERRORS.inc()
        print(f"ERROR >> Fallo insertando en network_data: {e}")
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando network_data: {e}")
    finally:
        SUPABASE_INSERT_IN_PROGRESS.dec()

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
        with SUPABASE_INSERT_TIME.time(), SUPABASE_INSERT_SUMMARY.time():
            SUPABASE_INSERT_IN_PROGRESS.inc()
            supabase.table("professional_data").upsert(data, on_conflict=["passport_fk"]).execute()
        write_log("INFO", "mongo_to_postgres.py", f"Professional_data insertado/actualizado: {data}")
    except Exception as e:
        SUPABASE_INSERT_ERRORS.inc()
        print(f"ERROR >> Fallo insertando en professional_data: {e}")
        write_log("ERROR", "mongo_to_postgres.py", f"Error insertando professional_data: {e}")
    finally:
        SUPABASE_INSERT_IN_PROGRESS.dec()

def buscar_passport_por_fullname_address(person, personas):
    """
    Busca el passport de otra persona agrupada que tenga el mismo fullname y/o address.
    """
    for p in personas:
        if p is person:
            continue
        # Coincidencia estricta por fullname y address
        if person.get("fullname") and p.get("fullname") == person.get("fullname"):
            if person.get("address") and p.get("address") == person.get("address"):
                if p.get("passport"):
                    return p.get("passport")
        # Coincidencia solo por fullname si no hay address
        if person.get("fullname") and p.get("fullname") == person.get("fullname") and p.get("passport"):
            return p.get("passport")
        # Coincidencia solo por address si no hay fullname
        if person.get("address") and p.get("address") == person.get("address") and p.get("passport"):
            return p.get("passport")
    return None

def buscar_location_id_por_address_city(person, personas, location_ids):
    """
    Busca el location_id de otra persona agrupada que tenga el mismo address y city.
    location_ids: dict {(address, city): location_id}
    """
    address = person.get("address")
    city = person.get("city")
    if address and city:
        return location_ids.get((address, city))
    return None

def fusionar_personas(collections):
    # Extrae y limpia todos los documentos
    personal = [clean_data(doc) for doc in collections["personal_data"].find()]
    location = [clean_data(doc) for doc in collections["location_data"].find()]
    professional = [clean_data(doc) for doc in collections["professional_data"].find()]
    bank = [clean_data(doc) for doc in collections["bank_data"].find()]
    net = [clean_data(doc) for doc in collections["net_data"].find()]

    personas_completas = []

    for p in personal:
        # Busca location por nombre y apellido en fullname
        loc = next((l for l in location if nombres_en_fullname(p.get("name"), p.get("last_name"), l.get("fullname"))), None)
        if not loc:
            continue  # No hay location, no se puede insertar

        # Busca professional por fullname
        prof = next((pr for pr in professional if pr.get("fullname") == loc.get("fullname")), None)
        # Busca bank por passport
        bnk = next((b for b in bank if b.get("passport") == p.get("passport")), None)
        # Busca net por address
        netw = next((n for n in net if address_match(n.get("address"), loc.get("address"))), None)

        # Solo inserta si tienes todos los datos
        if prof and bnk and netw:
            persona = {}
            persona.update(p)
            persona.update(loc)
            persona.update(prof)
            persona.update(bnk)
            persona.update(netw)
            personas_completas.append(persona)

    return personas_completas

def main():
    # Comprobaciones de conexión
    if not check_mongo_connection():
        print("❌ No se pudo conectar a MongoDB. Abortando.")
        return
    if not check_supabase_connection():
        print("❌ No se pudo conectar a Supabase. Abortando.")
        return

    collections = get_mongo_collections()
    personas = fusionar_personas(collections)
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    for person in personas:
        # 1. Insertar location
        location_id = insert_location(supabase, person)
        # 2. Insertar person
        insert_person(supabase, person, location_id)
        # 3. Insertar bank
        insert_bank(supabase, person)
        # 4. Insertar network
        insert_network(supabase, person, location_id)
        # 5. Insertar professional
        insert_professional(supabase, person)

    print(f"✅ Migración completada. Total personas completas insertadas: {len(personas)}")
    write_log("INFO", "mongo_to_postgres.py", f"Migración completada. Total personas completas insertadas: {len(personas)}")

if __name__ == "__main__":
    main()
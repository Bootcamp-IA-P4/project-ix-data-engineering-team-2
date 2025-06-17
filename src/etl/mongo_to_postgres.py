import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from supabase import create_client
from .etl_utils import clean_data
from ..utils.logg import write_log

# Cargar variables de entorno
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Inicializar conexiones
mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongo:27017/"))
mongo_db = mongo_client["raw_mongo_db"]
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def get_mongo_collections():
    """Obtiene todas las colecciones de MongoDB"""
    return {
        "personal_data": mongo_db["personal_data"],
        "location_data": mongo_db["location_data"],
        "professional_data": mongo_db["professional_data"],
        "bank_data": mongo_db["bank_data"],
        "net_data": mongo_db["net_data"]
    }

def migrate_collection(mongo_collection, supabase_table, unique_key):
    """Migra documentos de una colección de MongoDB a una tabla de Supabase"""
    docs = list(mongo_collection.find())
    if not docs:
        write_log("INFO", "mongo_to_postgres.py", f"No hay documentos nuevos en {mongo_collection.name}")
        return 0

    count = 0
    for doc in docs:
        # Limpia el _id de MongoDB para evitar problemas en Supabase
        doc.pop("_id", None)
        try:
            # Limpieza adicional si es necesario
            clean_doc = clean_data(doc)
            # Inserta o actualiza en Supabase
            supabase.table(supabase_table).upsert(clean_doc, on_conflict=[unique_key]).execute()
            count += 1
        except Exception as e:
            write_log("ERROR", "mongo_to_postgres.py", f"Error migrando doc en {supabase_table}: {e}")
    write_log("INFO", "mongo_to_postgres.py", f"Migrados {count} documentos de {mongo_collection.name} a {supabase_table}")
    return count

def main():
    write_log("INFO", "mongo_to_postgres.py", "==== Nuevo ciclo de migración iniciado ====")
    collections = get_mongo_collections()
    total = 0
    total += migrate_collection(collections["personal_data"], "persons", "passport")
    total += migrate_collection(collections["location_data"], "locations", "location_id")
    total += migrate_collection(collections["professional_data"], "professional_data", "passport_fk")
    total += migrate_collection(collections["bank_data"], "bank_data", "passport_fk")
    total += migrate_collection(collections["net_data"], "network_data", "passport_fk")
    if total == 0:
        write_log("INFO", "mongo_to_postgres.py", "No se migró ningún documento en este ciclo.")
    else:
        write_log("INFO", "mongo_to_postgres.py", f"Total de documentos migrados en este ciclo: {total}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        write_log("ERROR", "mongo_to_postgres.py", f"Error general en el ciclo de migración: {e}")
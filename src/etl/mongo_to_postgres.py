import os
from dotenv import load_dotenv
from pymongo import MongoClient
from supabase import create_client
from etl_utils import clean_data
from utils.logg import write_log


# Cargar variables de entorno
load_dotenv()

# Configuración de MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'raw_mongo_db')  # Cambiado a raw_mongo_db que es el que usas

# Configuración de Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

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

def migrate_collection(mongo_collection, supabase_table, unique_key):
    """Migra documentos de una colección de MongoDB a una tabla de Supabase"""
    print(f"MMM- Migrando colección {mongo_collection.name} a {supabase_table} +++")
    docs = list(mongo_collection.find({'processed': {'$ne': True}}))
    print(f"DDD- Encontrados {len(docs)}")
    if not docs:
        write_log("INFO", "mongo_to_postgres.py", f"No hay documentos nuevos en {mongo_collection.name}")
        return 0

    # Inicializar cliente de Supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    
    count = 0
    for doc in docs:
        # Limpia el _id de MongoDB para evitar problemas en Supabase
        doc.pop("_id", None)
        try:
            # Limpieza adicional si es necesario
            clean_doc = clean_data(doc)
            # Inserta o actualiza en Supabase
            supabase.table(supabase_table).upsert(clean_doc, on_conflict=[unique_key]).execute()
            
            # Marcar como procesado en MongoDB
            mongo_collection.update_one(
                {'_id': doc['_id']},
                {'$set': {'processed': True}}
            )
            
            count += 1
        except Exception as e:
            write_log("ERROR", "mongo_to_postgres.py", f"Error migrando doc en {supabase_table}: {e}")
    
    write_log("INFO", "mongo_to_postgres.py", f"Migrados {count} documentos de {mongo_collection.name} a {supabase_table}")
    return count

def main():
    try:
        print("++main++ Starting migration from MongoDB to Supabase...")
        write_log("INFO", "mongo_to_postgres.py", "==== Nuevo ciclo de migración iniciado ====")
        collections = get_mongo_collections()
        total = 0
        total += migrate_collection(collections["personal_data"], "persons", "passport")
        total += migrate_collection(collections["location_data"], "locations", "location_id")
        total += migrate_collection(collections["professional_data"], "professional_data", "passport_fk")
        total += migrate_collection(collections["bank_data"], "bank_data", "passport_fk")
        total += migrate_collection(collections["net_data"], "network_data", "passport_fk")
        
        if total == 0:
            print("000- No se migró ningún documento en este ciclo.")
            write_log("INFO", "mongo_to_postgres.py", "No se migró ningún documento en este ciclo.")
        else:
            print(f"+++- Total de documentos migrados en este ciclo: {total}")
            write_log("INFO", "mongo_to_postgres.py", f"Total de documentos migrados en este ciclo: {total}")

    except Exception as e:
        print(f"--- Error general en el ciclo de migración: {e}")
        write_log("ERROR", "mongo_to_postgres.py", f"Error general en el ciclo de migración: {e}")
        raise

if __name__ == "__main__":
    print("--->>>> Starting migration from MongoDB to Supabase...")
    main()
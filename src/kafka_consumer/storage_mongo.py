import os
from pymongo import MongoClient
from prometheus_client import Counter, Histogram, Gauge, Summary
import sys
from etl.utils.logg import write_log
MONGO_INSERTS = Counter('mongo_inserts_total', 'Total documentos insertados en MongoDB')
MONGO_ERRORS = Counter('mongo_insert_errors_total', 'Errores al insertar en MongoDB')
MONGO_INSERT_TIME = Histogram('mongo_insert_seconds', 'Tiempo en insertar documento en MongoDB')
MONGO_INSERT_IN_PROGRESS = Gauge('mongo_insert_in_progress', 'Documentos en proceso de inserción')
MONGO_INSERT_SUMMARY = Summary('mongo_insert_summary_seconds', 'Resumen de tiempos de inserción')
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:adminpassword@mongo:27017/")
client = None
db = None

def conectar_mongo():
    global client, db
    try:
        if client is None:
            client = MongoClient(MONGO_URI)
            db = client["raw_mongo_db"]
            write_log("INFO", "storage_mongo.py", "Conectado a MongoDB correctamente.")
    except Exception as e:
        write_log("ERROR", "storage_mongo.py", f"Error conectando a MongoDB: {e}")
        raise

def guardar_en_mongo(documento):
    try:
        with MONGO_INSERT_TIME.time(), MONGO_INSERT_SUMMARY.time():
            MONGO_INSERT_IN_PROGRESS.inc()
            keys = set(k.lower() for k in documento.keys())

            if "passport" in keys and "name" in keys:
                collection = db["personal_data"]
                filter_key = {"passport": documento.get("passport")}
            elif "fullname" in keys and "city" in keys:
                collection = db["location_data"]
                filter_key = {"fullname": documento.get("fullname")}
            elif "company" in keys or "job" in keys:
                collection = db["professional_data"]
                filter_key = {"fullname": documento.get("fullname")}
            elif "iban" in keys:
                collection = db["bank_data"]
                filter_key = {"passport": documento.get("passport")}
            elif "ipv4" in keys:
                collection = db["net_data"]
                # usar IPv4 como filtro único
                filter_key = {"IPv4": documento.get("IPv4")}
            else:
                collection = db["unknown_type"]
                filter_key = documento  # sin filtro único, podría cambiar
            # Actualizar o insertar el documento en la colección correspondiente
            collection.update_one(filter_key, {"$set": documento}, upsert=True)
            MONGO_INSERTS.inc()
            write_log("INFO", "storage_mongo.py", f"Documento guardado o actualizado en colección: {collection.name}")
    except KeyboardInterrupt:
        write_log("WARNING", "storage_mongo.py", "Inserción interrumpida por el usuario.")
        sys.exit(0)

    except Exception as e:
        MONGO_ERRORS.inc()
        write_log("ERROR", "storage_mongo.py", f"Error al guardar en MongoDB: {e}")
    finally:
        MONGO_INSERT_IN_PROGRESS.dec()


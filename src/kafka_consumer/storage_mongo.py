import os
from pymongo import MongoClient
from prometheus_client import Counter, Histogram, Gauge, Summary

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:adminpassword@mongo:27017/")
client = MongoClient(MONGO_URI)
db = client["raw_mongo_db"]

MONGO_INSERTS = Counter('mongo_inserts_total', 'Total documentos insertados en MongoDB')
MONGO_ERRORS = Counter('mongo_insert_errors_total', 'Errores al insertar en MongoDB')
MONGO_INSERT_TIME = Histogram('mongo_insert_seconds', 'Tiempo en insertar documento en MongoDB')
MONGO_INSERT_IN_PROGRESS = Gauge('mongo_insert_in_progress', 'Documentos en proceso de inserción')
MONGO_INSERT_SUMMARY = Summary('mongo_insert_summary_seconds', 'Resumen de tiempos de inserción')

def guardar_en_mongo(documento):
    try:
        with MONGO_INSERT_TIME.time(), MONGO_INSERT_SUMMARY.time():
            MONGO_INSERT_IN_PROGRESS.inc()
            #print("Guardando documento en MongoDB:", documento)
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

            collection.update_one(filter_key, {"$set": documento}, upsert=True)
            MONGO_INSERTS.inc()
            #print(f"✅ Documento guardado o actualizado en colección: {collection.name}")

    except Exception as e:
        MONGO_ERRORS.inc()
        #print("❌ Error al guardar en MongoDB :", e)
        print("")

    finally:
        MONGO_INSERT_IN_PROGRESS.dec()


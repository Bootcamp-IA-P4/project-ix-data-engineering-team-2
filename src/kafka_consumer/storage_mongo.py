import os
from pymongo import MongoClient
import sys
import os


from etl.utils.logg import write_log



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


# def write_log(level, module, message):
#     print(f"{level} - {module} - {message}")

def guardar_en_mongo(documento):
    try:
        if client is None or db is None:
            conectar_mongo()

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
            filter_key = {"IPv4": documento.get("IPv4")}
        else:
            collection = db["unknown_type"]
            filter_key = documento  # sin filtro único, puede cambiar

        collection.update_one(filter_key, {"$set": documento}, upsert=True)
        write_log("INFO", "storage_mongo.py", f"Documento guardado o actualizado en colección: {collection.name}")

    except Exception as e:
        write_log("ERROR", "storage_mongo.py", f"Error al guardar en MongoDB: {e}")


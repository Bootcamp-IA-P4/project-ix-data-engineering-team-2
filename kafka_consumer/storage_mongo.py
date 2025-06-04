from pymongo import MongoClient

client = MongoClient("mongodb://mongo:27017/")
db = client["raw_mongo_db"]  # Nombre de la base de datos
raw_collection = db["raw_messages"]

def guardar_en_mongo(documento):
    try:
        raw_collection.insert_one(documento)
        print("✅ Documento guardado en MongoDB.")
    except Exception as e:
        print("❌ Error al guardar en MongoDB:", e)

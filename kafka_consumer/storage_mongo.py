from pymongo import MongoClient

client = MongoClient("mongodb://mongo:27017/")
db = client["raw_mongo_db"]  # Nombre de la base de datos

# Guardar el mensaje crudo en MongoDB
def guardar_en_mongo(documento):
    try:
        keys = set(k.lower() for k in documento.keys())

        if "passport" in keys and "name" in keys:
            collection = db["personal_data"]
        elif "fullname" in keys and "city" in keys:
            collection = db["location_data"]
        elif "company" in keys or "job" in keys:
            collection = db["professional_data"]
        elif "iban" in keys:
            collection = db["bank_data"]
        elif "ipv4" in keys:
            collection = db["net_data"]
        else:
            collection = db["unknown_type"]

        collection.insert_one(documento)
        print(f"✅ Documento guardado en colección: {collection.name}")

    except Exception as e:
        print("❌ Error al guardar en MongoDB:", e)

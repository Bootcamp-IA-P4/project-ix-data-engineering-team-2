from pymongo import MongoClient

client = MongoClient("mongodb://mongo:27017/")
db = client["raw_mongo_db"]

def guardar_en_mongo(documento):
    try:
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
        print(f"✅ Documento guardado o actualizado en colección: {collection.name}")

    except Exception as e:
        print("❌ Error al guardar en MongoDB :", e)


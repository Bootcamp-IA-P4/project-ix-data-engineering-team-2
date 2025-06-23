# Este archivo permite consultar Redis desde fuera del ETL
# Se puede usar como módulo auxiliar o punto de partida para una API

import redis

# Conecta con el mismo contenedor Redis que en docker-compose
client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Busca el índice de persona a partir de una clave identificadora como 'email', 'passport', etc.
# Ejemplo de uso: get_person_index_by_key("email", "juan@example.com")
def get_person_index_by_key(key_name, value):
    redis_key = f"person_key:{value}"
    result = client.get(redis_key)
    if result is not None:
        return int(result)
    return None

def get_all_keys_for_debug():
    return client.keys()

def get_person_index(value):
    return client.get(f"person_key:{value}")

def set_person_index(value, index):
    client.set(f"person_key:{value}", index)

def clear_index():
    client.flushdb()

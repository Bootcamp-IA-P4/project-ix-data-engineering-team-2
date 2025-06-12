from collections import defaultdict

# Diccionario que mantendrá datos temporales
data_by_passport = defaultdict(dict)

def agregar_datos(nuevo_dato):
    passport = nuevo_dato.get("passport")

    if not passport:
        return None  # No se puede agrupar sin pasaporte

    data_by_passport[passport].update(nuevo_dato)

    # Regla simple: si hay al menos 5 claves, lo consideramos "completo"
    if len(data_by_passport[passport]) >= 5:
        datos_completos = data_by_passport[passport]
        del data_by_passport[passport]
        return datos_completos

    return None  # aún no hay suficientes datos

#Personal data, Location, Professional data, Bank Data y Net Data
from fastapi import FastAPI , Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from database.conect_database import conect
from urllib.parse import urlencode,urlparse
from sqlalchemy import func


app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def limpiar_nulos(diccionario):
    """
    Elimina claves con valor None de un diccionario, incluyendo los anidados.
    """
    limpio = {}
    for k, v in diccionario.items():
        if isinstance(v, dict):
            sub = limpiar_nulos(v)
            if sub:  # solo añade si no queda vacío
                limpio[k] = sub
        elif v is not None:
            limpio[k] = v
    return limpio

@app.get("/")
def read_root(request:Request):
    city = request.query_params.get("city") 
    job = request.query_params.get("job")
    resultado = conect.client.table("locations").select("city").execute()
    ciudades = list({item["city"] for item in resultado.data if item["city"]})  # elimina duplicados y vacíos
    ciudades.sort()  # opcional: ordena alfabéticamente
    trabajos = conect.client.table("professional_data").select("job_title").execute()
        # Obtener la lista de salarios desde los datos
    ltrabajos = list({item["job_title"] for item in trabajos.data if item["job_title"]})  # elimina duplicados y vacíos
    ltrabajos.sort()  # opcional: ordena alfabéticamente
    url = ""
    print(f"Ciudad: {city}, Trabajo: {job}")
    # Validar si hay datos
    if city.lower() != "none" and job.lower() != "none":
        url = f"/datos/{city}/{job}"
      

     
    return templates.TemplateResponse("index.html", {
        "request": request,
        "ciudades": ciudades,
        "trabajos": ltrabajos,
        "url": url,
        })


@app.get("/datos/{city}/{job}")
def buscar_personas_por_profesion_y_ciudad(job: str, city : str):
    if job.lower() == "all":
        job= ""
    if city.lower() == "all":
        city = ""
    query = conect.client.table("persons").select(
        "sex, location_id_fk,bank_data(*),professional_data(*),locations(address,city),network_data(*)"
    )
    if job: 
        query = query.ilike("professional_data.job_title", job.lower() + "%")
    if city:
        query = query.ilike("locations.city", city.lower() + "%")
    response = query.execute()

    # Filtrar solo los elementos que cumplen exactamente con el job y city
    print(f"Número de elementos devueltos por la consulta: {len(response.data) if response.data else 0}")
    resultados_filtrados = []
    for item in response.data or []:
        job_title = (
            item.get("professional_data", {}).get("job_title")
            if isinstance(item.get("professional_data"), dict)
            else None
        )
        ciudad = (
            item.get("locations", {}).get("city")
            if isinstance(item.get("locations"), dict)
            else None
        )
        if (not job or (job_title and job_title.startswith(job))) and (not city or (ciudad and ciudad.startswith(city))):
            resultados_filtrados.append(item)
    print(f"Número de elementos después de filtrar: {len(resultados_filtrados)}")
    personas_unidas = []
    for persona in resultados_filtrados:
        # Iniciar con los campos base (como 'sex')
        nueva_persona = {'sex': persona['sex']}

#Agregar todos los campos de 'professional_data' (excepto 'passport_fk')
        if 'professional_data' in persona:
            nueva_persona.update({
                k: v for k, v in persona['professional_data'].items()
                if k != 'passport_fk'
            })

#Agregar todos los campos de 'locations'
        if 'locations' in persona:
            nueva_persona.update(persona['locations'])

#Agregar todos los campos de 'bank_data' (excepto 'passport_fk')
        if 'bank_data' in persona:
            nueva_persona.update({
                k: v for k, v in persona['bank_data'].items()
                if k != 'passport_fk'
            })
#Agregar todos los campos de 'network_data' (excepto 'passport_fk')
        if 'network_data' in persona:
            nueva_persona.update({
                k: v for k, v in persona['network_data'].items()
                if k != 'passport_fk'
            })
        personas_unidas.append(nueva_persona)

    response.data = personas_unidas


    if not response.data or len(response.data) == 0:
        print("No se encontraron resultados para la búsqueda.")
        return {"mensaje": "No se encontraron resultados"}

    print("Resultados encontrados:")
    resultado_limpio = [limpiar_nulos(item) for item in response.data]
    campos_a_excluir = {"passport", "first_name", "last_name", "phone_number", "email", 'location_id_fk'}

    datos_filtrados = [
        {k: v for k, v in item.items() if k not in campos_a_excluir}
        for item in resultado_limpio
    ]



    return {"resultados": datos_filtrados}
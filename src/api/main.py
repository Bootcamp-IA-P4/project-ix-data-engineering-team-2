from fastapi import FastAPI , Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from database.conect_database import conect
from urllib.parse import urlencode,urlparse


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
def read_root(request:Request,genero: str, city: str, job: str):
    resultado = conect.client.table("locations").select("city").execute()
    ciudades = list({item["city"] for item in resultado.data if item["city"]})  # elimina duplicados y vacíos
    ciudades.sort()  # opcional: ordena alfabéticamente
    trabajos = conect.client.table("professional_data").select("job_title").execute()
        # Obtener la lista de salarios desde los datos
    ltrabajos = list({item["job_title"] for item in trabajos.data if item["job_title"]})  # elimina duplicados y vacíos
    ltrabajos.sort()  # opcional: ordena alfabéticamente
    #url = ""
    # Validar si hay datos
    #if not genero or not city or not job:
    url = f"/datos/{genero}/{city}/{job}"
      

     
    return templates.TemplateResponse("index.html", {
        "request": request,
        "ciudades": ciudades,
        "trabajos": ltrabajos,
        "url": url,
        })


@app.get("/datos/{genero}/{city}/{job}")
def filtrar_datos(genero: str, city: str, job: str):
    response = conect.client.table("persons") \
        .select("*,location_id(*),professional_data(*),bank_data(*),network_data(*)") \
        .eq("sex", genero) \
        .eq("city", city) \
        .execute()
    resultado_limpio = [limpiar_nulos(item) for item in response.data]
    campos_a_excluir = {"passport", "first_name", "last_name","phone_number","email"}

# Nueva lista de resultados sin esos campos
    datos_filtrados = [
        {k: v for k, v in item.items() if k not in campos_a_excluir}
        for item in resultado_limpio
    ]
    data = datos_filtrados

    if not data:
        return {"mensaje": "No se encontraron resultados"}

    return {"resultados": data}


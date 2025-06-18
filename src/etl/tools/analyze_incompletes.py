import json
import os
import time
from supabase import create_client

# Configuración de Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
INCOMPLETE_FILE = "incomplete_people.json"

# Campos clave para comparar
IDENT_KEYS = ['passport', 'email', 'fullname', 'telfnumber', 'name', 'last_name', 'address']

def load_incompletos():
    with open(INCOMPLETE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def fetch_supabase_persons():
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    resp = supabase.table("persons").select("*").execute()
    return resp.data

def match_any(record1, record2):
    """Devuelve True si hay algún campo clave igual entre dos registros."""
    for key in IDENT_KEYS:
        if key in record1 and key in record2 and record1[key] and record2[key]:
            if record1[key] == record2[key]:
                return True
    return False

def build_match_graph(records):
    """Construye un grafo de matches entre registros incompletos."""
    n = len(records)
    graph = {i: set() for i in range(n)}
    for i in range(n):
        for j in range(i+1, n):
            if match_any(records[i], records[j]):
                graph[i].add(j)
                graph[j].add(i)
    return graph

def find_connected_components(graph):
    """Encuentra componentes conexas (clusters de posibles personas)."""
    visited = set()
    components = []
    for node in graph:
        if node not in visited:
            stack = [node]
            comp = []
            while stack:
                curr = stack.pop()
                if curr not in visited:
                    visited.add(curr)
                    comp.append(curr)
                    stack.extend(graph[curr] - visited)
            components.append(comp)
    return components

def main():
    incompletos = load_incompletos()
    supa_persons = fetch_supabase_persons()

    print(f"Total incompletos: {len(incompletos)}")
    print(f"Total personas en Supabase: {len(supa_persons)}")

    # 1. Buscar matches directos con personas en Supabase
    for i, inc in enumerate(incompletos):
        for supa in supa_persons:
            if match_any(inc, supa):
                print(f"[MATCH DIRECTO] Incompleto #{i} coincide con persona en Supabase: {inc} <-> {supa}")

    # 2. Buscar matches entre incompletos (directos e indirectos)
    graph = build_match_graph(incompletos)
    components = find_connected_components(graph)
    print(f"\nComponentes conexas (posibles personas fusionables):")
    for comp in components:
        if len(comp) > 1:
            print(f"  - Registros {comp}:")
            for idx in comp:
                print(f"    {incompletos[idx]}")
            print("")

if __name__ == "__main__":
    while True:
        main()
        print("Esperando 20 segundos para volver a analizar...")
        time.sleep(20)
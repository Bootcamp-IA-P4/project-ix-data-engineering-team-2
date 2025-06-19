import unittest
import re

# --- Variables y funciones esenciales de tu código original ---
# (Estas copias son para que el test sea autocontenido y no dependa de tu archivo principal.
# En un proyecto real, las importarías directamente si el archivo está limpio de dependencias externas).

TITLES = ['mr', 'mrs', 'ms', 'sr', 'sra', 'dr', 'dott', 'sig', 'mtro', 'sr(a)', 'mrs.', 'dr.', 'ms.', 'sr.', 'sig.']
identifying_keys = ['passport', 'email', 'fullname', 'telfnumber', 'name', 'last_name', 'address']

# Simulamos las variables globales para los tests de fusión
people_data = []
person_index = {}

def write_log(level, module, message):
    # Función de log simulada para que los tests no fallen si no tienes un logg.py
    pass

def clean_name(name):
    if not isinstance(name, str):
        return name
    name_clean = name.lower()
    for title in TITLES:
        name_clean = re.sub(rf'\b{re.escape(title)}\b', '', name_clean, flags=re.IGNORECASE)
    name_clean = re.sub(r'[().]', '', name_clean)
    return name_clean.strip().title()

def clean_telf(telf):
    if not isinstance(telf, str):
        return telf
    return re.sub(r'[^+0-9]', '', telf)

def clean_address_field(text):
    if not isinstance(text, str):
        return text
    return text.replace(',', '')

def clean_data(record):
    if 'name' in record:
        record['name'] = clean_name(record['name'])
    if 'telfnumber' in record:
        record['telfnumber'] = clean_telf(record['telfnumber'])
    if 'company_telfnumber' in record:
        record['company_telfnumber'] = clean_telf(record['company_telfnumber'])
    if 'city' in record:
        record['city'] = clean_address_field(record['city'])
    if 'address' in record:
        record['address'] = clean_address_field(record['address'])
    if 'company address' in record:
        record['company address'] = clean_address_field(record['company address'])
    if 'sex' in record:
        value = record['sex']
        if isinstance(value, list) and len(value) > 0:
            record['sex'] = str(value[0])
        elif isinstance(value, str):
            record['sex'] = value
    return record

def find_all_person_indices(record):
    indices = set()
    for key in identifying_keys:
        if key in record and record[key] in person_index:
            indices.add(person_index[record[key]])
    return list(indices)

def merge_people(indices):
    global people_data, person_index
    if not indices:
        return None
    indices = sorted(indices)
    base_idx = indices[0]
    for idx in indices[1:]:
        people_data[base_idx].update(people_data[idx])
    for idx in sorted(indices[1:], reverse=True):
        del people_data[idx]
    person_index.clear()
    for idx, person in enumerate(people_data):
        for key in identifying_keys:
            if key in person:
                person_index[person[key]] = idx
    return base_idx

def register_keys(record, idx):
    global person_index
    for key in identifying_keys:
        if key in record:
            person_index[record[key]] = idx

def group_records(records):
    global people_data, person_index
    for record in records:
        indices = find_all_person_indices(record)
        if indices:
            base_idx = merge_people(indices)
            people_data[base_idx].update(record)
            register_keys(people_data[base_idx], base_idx)
        else:
            idx = len(people_data)
            people_data.append(record)
            register_keys(record, idx)



### Tests Unitarios Simplificados


class TestETLFunctionsSimplified(unittest.TestCase):

    def setUp(self):
        """Reinicia las variables globales antes de cada test para un estado limpio."""
        global people_data, person_index
        people_data = []
        person_index = {}

    
    ## Tests de Limpieza de Datos
    

    def test_clean_name_simple(self):
        # Prueba básica de limpieza de nombre con título
        self.assertEqual(clean_name("Mr. John Doe"), "John Doe")
        # Prueba con mayúsculas y sin título
        self.assertEqual(clean_name("JANE SMITH"), "Jane Smith")
        # Prueba con valor no string
        self.assertEqual(clean_name(123), 123)

    def test_clean_telf_simple(self):
        # Prueba de limpieza de número de teléfono
        self.assertEqual(clean_telf("+34 678-123-456"), "+34678123456")
        # Prueba con número ya limpio
        self.assertEqual(clean_telf("1234567890"), "1234567890")
        # Prueba con valor no string
        self.assertEqual(clean_telf(None), None)

    def test_clean_data_main_flow(self):
        # Prueba integral de clean_data con campos comunes
        record = {
            'name': 'Dr. ALICE SMITH',
            'telfnumber': ' (111) 222-3333 ',
            'address': '123 Fake St, Apt 4',
            'sex': ['female']
        }
        cleaned_record = clean_data(record.copy())
        self.assertEqual(cleaned_record['name'], 'Alice Smith')
        self.assertEqual(cleaned_record['telfnumber'], '1112223333')
        self.assertEqual(cleaned_record['address'], '123 Fake St Apt 4')
        self.assertEqual(cleaned_record['sex'], 'female')

    
    ## Tests de Agrupación de Registros (Lógica de Fusión)
    

    def test_group_records_add_new_person(self):
        # Verifica que un registro nuevo se añade correctamente
        record = {'passport': 'P123', 'name': 'Nuevo Usuario'}
        group_records([record])
        self.assertEqual(len(people_data), 1)
        self.assertEqual(people_data[0]['name'], 'Nuevo Usuario')
        self.assertEqual(person_index['P123'], 0)

    def test_group_records_merge_existing_person(self):
        # Verifica que registros relacionados se fusionan
        # 1. Añade el primer registro
        initial_record = {'email': 'test@example.com', 'name': 'Inicial'}
        group_records([initial_record])
        self.assertEqual(len(people_data), 1)
        self.assertEqual(people_data[0]['name'], 'Inicial')
        self.assertEqual(person_index['test@example.com'], 0)

        # 2. Añade un segundo registro con la misma clave de identificación (email)
        #    Debería fusionarse con el existente.
        second_record = {'email': 'test@example.com', 'passport': 'ABC'}
        group_records([second_record])
        self.assertEqual(len(people_data), 1)  # Todavía debe haber 1 persona
        self.assertEqual(people_data[0]['name'], 'Inicial')
        self.assertEqual(people_data[0]['passport'], 'ABC') # El nuevo dato debe estar
        self.assertEqual(person_index['ABC'], 0) # La nueva clave debe apuntar al mismo índice

    def test_group_records_merge_multiple_keys(self):
        # Prueba fusionando con dos registros existentes a través de diferentes claves
        global people_data, person_index
        people_data = [
            {'passport': 'P1', 'name': 'Persona A'},
            {'email': 'a@b.com', 'address': 'Calle Falsa'}
        ]
        person_index = {
            'P1': 0,
            'a@b.com': 1,
            'Calle Falsa': 1
        }

        # Un registro que tiene claves de ambos pre-existentes
        merging_record = {'passport': 'P1', 'email': 'a@b.com', 'job': 'Engineer'}
        group_records([merging_record])

        self.assertEqual(len(people_data), 1) # Debería fusionarse en una sola persona
        expected_data = {
            'passport': 'P1',
            'name': 'Persona A',
            'email': 'a@b.com',
            'address': 'Calle Falsa',
            'job': 'Engineer'
        }
        self.assertEqual(people_data[0], expected_data)
        self.assertEqual(person_index['P1'], 0)
        self.assertEqual(person_index['a@b.com'], 0)

# Para ejecutar los tests
if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
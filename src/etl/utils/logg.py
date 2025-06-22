import os
from datetime import datetime
import json
''''
Ejemplo para escribir en el log:
    1º importar la función write_log
        from src.utils.logg import write_log
    2º llamar a la función con los parámetros necesarios:
        write_log("ERROR", "logg.py", "This is a test log entry.")
'''

LOG_PATH = os.path.join(
    os.path.dirname(__file__),
    "logs",
    f"{datetime.now().strftime('%Y-%m-%d')}.log"
)


def write_log(level, source,text):
    try:
        if not os.path.exists(os.path.dirname(LOG_PATH)):
            os.makedirs(os.path.dirname(LOG_PATH))
        with open(LOG_PATH, "a", encoding="utf-8") as log:
            log_entry = {
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "level": level,
                "source": source,
                "message": text
            }
            log.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Error writing log: {e}")


write_log("ERROR", "logg.py", "This is a test log entry.")
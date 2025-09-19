import os
import requests

# Configuración
DIRECTORIO = r"./tweets"
file_start = "tweets_part"
OPENSEARCH_URL = "http://localhost:9200/_bulk"
HEADERS = {"Content-Type": "application/json"}

for filename in os.listdir(DIRECTORIO):
    if filename.startswith(file_start) and filename.endswith(".json"):
        file_path = os.path.join(DIRECTORIO, filename)
        print(f"Subiendo {file_path}...")

        with open(file_path, "rb") as f:
            data = f.read()
       
        response = requests.post(OPENSEARCH_URL, headers=HEADERS, data=data)

        if response.status_code == 200:
            result = response.json()
            if result.get("errors"):
                print(f"⚠️ Error en la carga de {filename}: Hay errores en el bulk.")
            else:
                print(f"✅ {filename} cargado correctamente.")
        else:
            print(f"❌ Falló la carga de {filename}. Status code: {response.status_code}")
            print(response.text)

import csv
import json
import requests
import os

def convert_value(value):
    try:
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        return value

csv_file = "../data/dataset_23_10.csv"
json_file = "dataset_23_10.json"
index_name = "tweets_23_10"

# sino al crear el índice en Elasticsearch. Aquí solo defines los datos y el índice destino.
# Si quieres definir el mapping, debes hacerlo antes de cargar los datos, usando la API de Elasticsearch.
# Ejemplo de cómo sería el mapping en Python (usando requests):

mapping = {
    "mappings": {
        "properties": {
        "rt_user_id": {
        "type": "text",
        "fielddata": 'true',
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      }
    }
    }
}
resp = requests.put(f"http://localhost:9200/{index_name}", json=mapping)

with open(csv_file, "r", encoding="utf-8") as f_csv, open(json_file, "w", encoding="utf-8") as f_json:
    reader = csv.DictReader(f_csv)
    for row in reader:
        # Convert each value to int/float if possible
        converted_row = {k: convert_value(v) for k, v in row.items()}
        
        
        action = {
            "index": {
            "_index": index_name,
            # "_id": converted_row.get("user_id")  # Si quieres un id personalizado
            }
        }
        f_json.write(json.dumps(action) + "\n")
        f_json.write(json.dumps(converted_row, ensure_ascii=False) + "\n")
        

#%% Divide bulk file        

lines_per_file = 10000  # 5000 documentos
outname_part = "tweets_part_"
with open(json_file, "r", encoding="utf-8") as f:
    part = 1
    buffer = []
    output_dir = "tweets"
    os.makedirs(output_dir, exist_ok=True)
    
    for i, line in enumerate(f, 1):
        buffer.append(line)
        if i % lines_per_file == 0:
            outname = os.path.join(output_dir, f"{outname_part}{part}.json")
            # outname = f"tweets_and_retweets_part_{part}.json"
            with open(outname, "w", encoding="utf-8") as out:
                out.writelines(buffer)
            print(f"Escribiendo {outname}")
            buffer = []
            part += 1

            if buffer:
                outname = os.path.join(output_dir, f"{outname_part}{part}.json")
                with open(outname, "w", encoding="utf-8") as out:
                    out.writelines(buffer)
                print(f"Escribiendo {outname}")
# %% Upload bulk files to OpenSearch

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

# %%

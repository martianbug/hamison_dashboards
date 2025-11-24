import csv
import json
import requests
import os
import ast


USER="admin"
PWD="admin"
URL=f"https://{USER}:{PWD}@localhost:8888"


def convert_value(v):
    if v == "" or v.lower() in ("none", "nan"):
        return None
    try:
        val = ast.literal_eval(v)
        if val is Ellipsis:  # ⚠️ Manejo explícito
            return None
        if isinstance(val, complex):
            # Opción 1: guardarlo como string
            return str(val)
        if isinstance(val, list):
            return [str(x).upper() for x in val]
        if isinstance(val, bool):
            return str(val).upper()
        return val
    except:
        pass
    
    try:
        if "." in v:
            return float(v)
        return int(v)
    except:
        return v  # deja como texto si no se puede convertir

csv_file = "../data/dataset_3_11_extended_es_hashtags_fixed.csv"
json_file = "dataset_5_11_es.json"
index_name = "tweets_es_obj"

upload_mapping = False  # Cambiar a True si se quiere subir el mapping
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
if upload_mapping:
    # resp = requests.put(f"http://localhost:9200/{index_name}", json=mapping)
    resp = requests.put(f"{URL}/{index_name}", json=mapping)

print("Convirtiendo CSV a formato bulk para OpenSearch...")
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
outname_part = index_name + "_part_"

print("Dividiendo el archivo bulk en partes...")
with open(json_file, "r", encoding="utf-8") as f:
    part = 1
    buffer = []
    output_dir = index_name
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

DIRECTORIO = r"./" + index_name
file_start = f"{index_name}_part"

OPENSEARCH_URL = f"{URL}/_bulk"
HEADERS = {"Content-Type": "application/json"}

print("Subiendo archivos bulk a OpenSearch...")
for filename in os.listdir(DIRECTORIO):
    if filename.startswith(file_start) and filename.endswith(".json"):
        file_path = os.path.join(DIRECTORIO, filename)
        print(f"Subiendo {file_path}...")
        with open(file_path, "rb") as f:
            data = f.read()
       
        response = requests.post(OPENSEARCH_URL, headers=HEADERS, data=data, verify=False)

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

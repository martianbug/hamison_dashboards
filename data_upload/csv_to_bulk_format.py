import csv
import json
import requests

def convert_value(value):
    try:
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        return value

csv_file = "../dataset_17_09.csv"
json_file = "dataset_14_10.json"
index_name = "tweets_14_10"

# sino al crear el índice en Elasticsearch. Aquí solo defines los datos y el índice destino.
# Si quieres definir el mapping, debes hacerlo antes de cargar los datos, usando la API de Elasticsearch.
# Ejemplo de cómo sería el mapping en Python (usando requests):

# mapping = {
#     "mappings": {
#         "properties": {
#         "rt_user_id": {
#         "type": "text",
#         "fielddata": 'true',
#         "fields": {
#           "keyword": {
#             "type": "keyword"
#           }
#         }
#       }
#     }
#     }
# }
# resp = requests.put(f"http://localhost:9200/{index_name}", json=mapping)

# print(resp.json())

        
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
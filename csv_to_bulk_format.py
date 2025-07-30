import csv
import json

csv_file = "usuarios.csv"
json_file = "usuarios2.json"
index_name = "usuarios2"
with open(csv_file, "r", encoding="utf-8") as f_csv, open(json_file, "w", encoding="utf-8") as f_json:
    reader = csv.DictReader(f_csv)
    
    for row in reader:
        action = { "index": { "_index": index_name } }
        f_json.write(json.dumps(action) + "\n")
        f_json.write(json.dumps(row, ensure_ascii=False) + "\n")
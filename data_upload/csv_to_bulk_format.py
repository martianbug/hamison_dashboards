import csv
import json

def convert_value(value):
    try:
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        return value

csv_file = "../usuarios_10_09.csv"
json_file = "usuarios_10.json"
index_name = "usuarios_10"

with open(csv_file, "r", encoding="utf-8") as f_csv, open(json_file, "w", encoding="utf-8") as f_json:
    reader = csv.DictReader(f_csv)
    for row in reader:
        # Convert each value to int/float if possible
        converted_row = {k: convert_value(v) for k, v in row.items()}
        action = { "index": { "_index": index_name } }
        f_json.write(json.dumps(action) + "\n")
        f_json.write(json.dumps(converted_row, ensure_ascii=False) + "\n")
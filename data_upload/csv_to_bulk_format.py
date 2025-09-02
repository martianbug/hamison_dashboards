import csv
import json

csv_file = "../dataset_01_09.csv"
json_file = "tweets_and_retweets.json"
index_name = "tweets-and-retweets"
with open(csv_file, "r", encoding="utf-8") as f_csv, open(json_file, "w", encoding="utf-8") as f_json:
    reader = csv.DictReader(f_csv)
    
    for row in reader:
        action = { "index": { "_index": index_name } }
        f_json.write(json.dumps(action) + "\n")
        f_json.write(json.dumps(row, ensure_ascii=False) + "\n")
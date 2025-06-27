#%%
import json
import pandas as pd
import json

df = pd.read_csv("usuarios.csv")

index_name = "usuarios-twitter"

with open("bulk_data.json", "w", encoding='utf-8') as f:
    for i, row in df.iterrows():
        # Línea de metadatos
        meta = {
            "index": {
                "_index": index_name,
                "_id": str(row['user_id'])  # Usa otro campo si no tienes 'user_id'
            }
        }
        f.write(json.dumps(meta) + "\n")
        
        doc = row.dropna().to_dict()  # elimina NaNs
        f.write(json.dumps(doc, default=str) + "\n")  # default=str evita errores con fechas

#%%
from opensearchpy import OpenSearch, helpers
import pandas as pd
import json

client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_compress=True,
    timeout=30
)
df = pd.read_csv("usuarios.csv")  # o usar un DataFrame que ya tienes

def generate_bulk_actions(df, index_name):
    for _, row in df.iterrows():
        doc = row.dropna().to_dict()
        yield {
            "_index": index_name,
            "_id": str(row['user_id']),  # O usa otro campo como ID
            "_source": doc
        }

index_name = "usuarios-twitter"

response = helpers.bulk(client, generate_bulk_actions(df, index_name))
print("Bulk insert completado:", response)
#%% CONSULTAS
# Consulta con filtros (por ejemplo: num_tweets ≥ 10 y sentimiento positivo)
query = {
    "size": 100,
    "query": {
        "bool": {
            "must": [
                {"range": {"num_tweets": {"gte": 1}}},
                {"term": {"majority_sentiment.keyword": "neutral"}}
            ]
        }
    }
}

res = client.search(index="usuarios-twitter", body=query)

docs = [hit["_source"] for hit in res["hits"]["hits"]]
print(docs)
#%%
# Agregaciones: Conteo por sentimiento
agg_query = {
    "size": 0,
    "aggs": {
        "sentimiento_count": {
            "terms": {
                "field": "majority_sentiment.keyword"
            }
        }
    }
}

res = client.search(index="usuarios-twitter", body=agg_query)
for bucket in res["aggregations"]["sentimiento_count"]["buckets"]:
    print(bucket["key"], bucket["doc_count"])

#%% Buscar un usuario por nombre (texto completo)
query = {
    "query": {
        "match": {
            "user_name": "juan"
        }
    }
}

res = client.search(index="usuarios-twitter", body=query)
for hit in res["hits"]["hits"]:
    print(hit["_source"])


# %% Convertir a dataframe
import pandas as pd

df = pd.DataFrame([hit["_source"] for hit in res["hits"]["hits"]])

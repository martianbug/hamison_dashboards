#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 Alberto Pérez García-Plaza
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Alberto Pérez García-Plaza <alberto.perez@lsi.uned.es>
#
import datetime
import time

import dateutil.parser as du_parser
import json
import sys

from pathlib import Path

# from data_explorer.data.indexer import Indexer
# from data_explorer.data import util

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
        
        # Línea del documento
        doc = row.dropna().to_dict()  # elimina NaNs
        f.write(json.dumps(doc, default=str) + "\n")  # default=str evita errores con fechas

#%%

from opensearchpy import OpenSearch, helpers
import pandas as pd
import json

# 1. Conexión a OpenSearch
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_compress=True,
    timeout=30
)

# 2. Cargar el DataFrame
df = pd.read_csv("usuarios.csv")  # o usar un DataFrame que ya tienes

# 3. Preparar los documentos para la API bulk
def generate_bulk_actions(df, index_name):
    for _, row in df.iterrows():
        doc = row.dropna().to_dict()
        yield {
            "_index": index_name,
            "_id": str(row['user_id']),  # O usa otro campo como ID
            "_source": doc
        }

# 4. Insertar en OpenSearch
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
                {"range": {"num_tweets": {"gte": 10}}},
                {"term": {"majority_sentiment.keyword": "positive"}}
            ]
        }
    }
}

res = client.search(index="usuarios-twitter", body=query)
docs = [hit["_source"] for hit in res["hits"]["hits"]]
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

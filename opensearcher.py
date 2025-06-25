from opensearchpy import OpenSearch
import pandas as pd

# Conexión al cluster de OpenSearch
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_compress=True,
    timeout=30
)

INDEX = "usuarios-twitter"  # nombre del índice

min_tweets = 10
min_retweets = 0
max_age = 10000
sentimientos = ["positive", "neutral", "negative"]

# Construcción de la consulta
query = {
    "size": 1000,
    "query": {
        "bool": {
            "must": [
                {"range": {"num_tweets": {"gte": min_tweets}}},
                {"range": {"num_retweets": {"gte": min_retweets}}},
                {"range": {"user_age_days": {"lte": max_age}}},
                {"terms": {"majority_sentiment.keyword": sentimientos}}
            ]
        }
    }
}

response = client.search(index=INDEX, body=query)

# Procesamiento de resultados
hits = response['hits']['hits']
data = [hit['_source'] for hit in hits]

# Conversión a DataFrame
df = pd.DataFrame(data)

# Mostrar algunos resultados
print(df.head())

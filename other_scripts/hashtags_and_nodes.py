#%% INTRO
import webbrowser
import pandas as pd
from utilities import string_to_list, normalize_column
import networkx as nx

DATE = '06_05'
NAME = 'dataset_' + DATE
COLUMN_TO_ANALYZE = 'pysentimiento'
NUMBER_OF_NODES =  5000

df = pd.read_csv(NAME+'.csv')
df['hashtags'] = df['hashtags'].apply(string_to_list)
df['hashtags'] = df['hashtags'].apply(normalize_column)
df_exploded = df.explode('hashtags')

top_n = 50  # Número de hashtags más comunes
top_hashtags = df_exploded['hashtags'].value_counts().head(top_n).index
df_filtered = df_exploded[df_exploded['hashtags'].isin(top_hashtags)]

edges_df = df_filtered[['user_id', 'hashtags']].dropna().drop_duplicates()
edges_df.columns = ['user', 'hashtag'] 

selection_test = edges_df.sample(NUMBER_OF_NODES)
edges_df = selection_test.copy()

users = edges_df['user'].unique()
hashtags = edges_df['hashtag'].unique()

B = nx.Graph()
B.add_nodes_from(users, bipartite='users', color = 'blue')
B.add_nodes_from(hashtags, bipartite='hashtags', color = 'green')

# Añadir aristas: cada usuario conectado al hashtag que usó
B.add_edges_from(edges_df.itertuples(index=False))

hashtag_nodes = [n for n in B.nodes if B.nodes[n].get('bipartite') == 'hashtags']
user_nodes = [n for n in B.nodes if B.nodes[n].get('bipartite') == 'users']
#%% Hashtags df with COLUMN_TO_ANALYZE
df = df[df['hashtags'].map(len) > 0]

df_majority = (
    df_filtered.groupby('hashtags')
    .agg(
        majority_sentiment = (COLUMN_TO_ANALYZE, lambda x: pd.Series.mode(x)[0]),
        user_ids=('user_id', lambda x: list(x.unique()))
    )
    .reset_index()
    .rename(columns={'hashtags': 'hashtag'})
)
hashtag_sentiment = (
    df_filtered.groupby('hashtags')['pysentimiento']
    .agg(lambda x: pd.Series.mode(x)[0])
    .reset_index()
    .rename(columns={'hashtags': 'hashtag', 'pysentimiento': 'majority_sentiment'})
)

# Frecuencia de cada hashtag (número de veces que aparece)
hashtag_counts = df_filtered['hashtags'].value_counts().reset_index()
hashtag_counts.columns = ['hashtag', 'count']

# Unir en un solo DataFrame
hashtag_info = pd.merge(hashtag_sentiment, hashtag_counts, on='hashtag')
#%% COLOREAR SEGÚN FECHA DE CREACIÓN DEL USUARIO Y SENTIMIENTO DEL HASHTAG
import matplotlib.cm as cm

# Agrupar por 'id' de usuarios y calcular el sentimiento mayoritario
user_sentiment_map = (
    df.groupby('user_id')['pysentimiento']  
    .agg(lambda x: pd.Series.mode(x)[0])
    .to_dict()
)
# Mapeo de sentimientos a colores
sentiment_color_map = {
    'POS': 'lightgreen',
    'NEG': 'salmon',
    'NEU': 'lightblue'
}
hashtag_color_dict = {
    row['hashtag']: sentiment_color_map.get(row['majority_sentiment'], 'gray')
    for _, row in hashtag_sentiment.iterrows()
}

max_count = hashtag_info['count'].max()
hashtag_size_dict = {
    row['hashtag']: 100 * (row['count'] / max_count) + 10  # escala entre 100 y 400
    for _, row in hashtag_info.iterrows()
}
#%% DIBUJAR usando nx
# import matplotlib.pyplot as plt
# plt.figure(figsize=(20, 14))
# pos = nx.spring_layout(B, k=0.7, iterations=50, seed=42)

# # pos = nx.circular_layout(B)
# # Colores y tamaños para hashtags
# hashtag_colors = [hashtag_color_dict.get(n, 'gray') for n in hashtag_nodes]
# hashtag_sizes = [hashtag_size_dict.get(n, 100) for n in hashtag_nodes]

# # Dibujar usuarios coloreados por antigüedad
# nx.draw_networkx_nodes(B, pos,
#                        nodelist=user_nodes,
#                        node_color=user_node_colors,
#                        node_size=40,
#                        alpha=0.7)

# # Dibujar hashtags (coloreados y escalados)
# nx.draw_networkx_nodes(B, pos,
#                        nodelist=hashtag_nodes,
#                        node_color=hashtag_colors,
#                        node_size=hashtag_sizes,
#                        alpha=0.8)

# # Dibujar edges
# nx.draw_networkx_edges(B, pos,
#                        width=0.3,
#                        alpha=0.5,
#                        edge_color='gray')

# # Dibujar etiquetas de hashtags con desplazamiento
# label_pos = {n: (x, y + 0.03) for n, (x, y) in pos.items() if n in hashtag_nodes}
# labels = {n: n for n in hashtag_nodes}
# nx.draw_networkx_labels(B, label_pos, labels,
#                         font_size=10,
#                         font_color='black')

# plt.title("Red bipartita: usuarios coloreados por antigüedad, hashtags coloreados según sentimiento y tamaño según uso", fontsize=16)
# plt.axis('off')
# plt.tight_layout()
# plt.show()

# %% DIBUJAR USANDO PYVIS
from pyvis.network import Network
import networkx as nx
import matplotlib.colors as mcolors

net = Network(height='800px', 
              width='100%', 
              bgcolor='#ffffff', 
              font_color='black',
              select_menu=True,
              filter_menu=True
              )

B_cleaned = nx.Graph()

for n, attrs in B.nodes(data=True):
    n_clean = str(n)
    if attrs.get('bipartite') == 'hashtags':
      label = str(n_clean)
      attrs['label'] = label
    B_cleaned.add_node(n_clean, **attrs)

for u, v in B.edges():
    B_cleaned.add_edge(str(u), str(v))
    
partition = nx.community.greedy_modularity_communities(B_cleaned)
modularity = nx.community.modularity(B_cleaned, partition)
degree_centrality = nx.degree_centrality(B_cleaned)
# betweenness_centrality = nx.betweenness_centrality(B_cleaned, normalized=True)
# nx.set_node_attributes(B_cleaned, degree_centrality, 'centrality')

num_communities = len(partition)
colormap = cm.get_cmap('tab20', num_communities) 
community_map = {}
for i, community in enumerate(partition):
    for node in community:
        community_map[node] = i

net.from_nx(B_cleaned) # Convertir el grafo de networkx a pyvis

for node in B_cleaned.nodes():
    community_id = community_map.get(node, -1)  # -1 si no se encuentra
    color = '#dddddd'  # default gray
    if community_id >= 0:
        rgba = colormap(community_id)
        color = mcolors.to_hex(rgba)
        
#%% ADD NODES
for node in net.nodes:
    tipo = B_cleaned.nodes[node['id']].get('bipartite')
    if tipo == 'users':
        # sentiment = user_sentiment_map.get(float(node['id']), 'neutral')
        # color = {
        # 'POS': 'lightgreen',
        # 'NEG': 'salmon',
        # 'NEU': 'lightblue'
        # }.get(sentiment, 'blue')
        # deg_cent = degree_centrality.get(node['id'], 0)
        # size = 10 + deg_cent * 40  # tamaño mínimo 10, aumenta con centralidad
        # node['size'] = size
        community_id = community_map.get(node['id'], -1)  # -1 si no se encuentra
        color = '#dddddd'  # default gray
        if community_id >= 0:
            rgba = colormap(community_id)
            color = mcolors.to_hex(rgba)

        node['color'] = color
        user_info = df[df['user_id'] == node['id']].dropna(subset=['user_created_at'])
        if not user_info.empty:
            created = user_info['user_created_at'].iloc[0]
            node['title'] = f"Usuario creado en: {created}"
    elif tipo == 'hashtags':
        # node['color'] = 'green'
        node['color'] = hashtag_color_dict.get(node['id'], 'gray')
        print(node['color'])
        deg_cent = degree_centrality.get(node['id'], 0)
        size = 10 + deg_cent * 40  # tamaño mínimo 10, aumenta con centralidad
        node['size'] = size
        # node['size'] = hashtag_size_dict.get(node['id'], 100)
        node['title'] = f"Hashtag: {node['id']}"
#%%
net.set_options("""
var options = {
  "layout": {"improvedLayout": false},
  "physics": {
    "forceAtlas2Based": {
      "gravitationalConstant": -30,
      "centralGravity": 0.05,
      "springLength": 120,
      "springConstant": 0.08
    },
    "solver": "forceAtlas2Based",
    "timestep": 0.1,
    "stabilization": {"enabled": true, "iterations": 150}
  }
}
""")    

# net.force_atlas_2based(gravity=-30, central_gravity=0.01, spring_length=100, spring_strength=0.05)
net.show('red_interactiva.html', notebook = False)

#%%
# Insertar título modificando el HTML generado
with open('red_interactiva.html', 'r') as f:
    html = f.read()

titulo = f"<h2 style='text-align:center;'>Red de usuarios y hashtags con {NUMBER_OF_NODES} nodos.</h2>"
titulo += "<h3 style='text-align:center;'>Coloreados según modularidad. Tamaño nodos centrales según centralidad</h3>"
html = html.replace('<body>', f'<body>{titulo}', 1)

with open('red_interactiva_con_titulo.html', 'w', encoding='utf-8') as f:
    f.write(html)
webbrowser.open('red_interactiva_con_titulo.html')
# %%

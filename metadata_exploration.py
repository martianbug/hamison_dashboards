#%% INTRO
import webbrowser
import pandas as pd
import networkx as nx
import matplotlib.cm as cm
import numpy as np

def classify_node(row):
    if row['num_posts'] > 20 and row['user_age_days'] > 1000:
        return 'core'
    elif row['num_posts'] > 5:
        return 'regular'
    else:
        return 'peripheral'

DATE = '26_05'
# DATE = '04_06'

NAME = 'dataset_' + DATE
SUBSET_SIZE = 1000

# num de repeticiones//ordenar usuarios por sentimiento

# crear un csv de usuarios con metricas: num publicaciones, num retweets, lista de ids de mensajes, sentimiento agregado/emocion agregada, num RT que hacen  
# num rt que reciben/
# buscar metricas de centralidad o de data analysis
# lista de ids de usuarios que ha RT. ordenar por cantidad orden de RT.

#  en el primer csv: csv id de mensaje - usuario id : para crear esos edges

#TODO: me falta informacion de los edges que necesitamos: respuestas? x ej

df = pd.read_csv(NAME + '.csv')
df_sentimient = df[['id', 'pysentimiento']].copy()
# df = df[df['lang'].isin(['es'])] #filter for spanish tweets
df2 = pd.read_csv('tweets_with_groups_and_urls_all' + '.csv')

df['user_id'] = df['user_id'].astype(str)
df['rt_user_id'] = df['rt_user_id'].replace(['', 'None', None, 0], np.nan)

only_tweets = df[df['rt_user_id'].isna()]
retweets_df = df[df['rt_user_id'].notna()]

retweets_df['rt_user_id'] = retweets_df['rt_user_id'].astype('Int64')

user_names = df.groupby('user_id').agg(user_name=('user_name', 'first'))

tweets_por_usuario = only_tweets.groupby('user_id').agg(num_tweets=('id', 'count'), tweet_ids=('id', lambda x: list(x))).reset_index()
retweets_por_usuario = retweets_df.groupby('user_id').agg(num_retweets=('id', 'count'), retweet_ids=('id', lambda x: list(x))).reset_index()

retweeters_por_usuario = (
    retweets_df.groupby('rt_user_id')['user_id']
    .nunique()
    .reset_index()
    .rename(columns={'rt_user_id': 'user_id', 'user_id': 'num_retweeters'})
)
retweeters_por_usuario['user_id'] = retweeters_por_usuario['user_id'].astype(str)

df_usuarios = (
    tweets_por_usuario
    .merge(retweets_por_usuario, on='user_id', how='outer')
    .merge(user_names, on='user_id', how='left')
    .fillna({'num_retweets': 0, 'retweet_ids': ''})
)
df_usuarios = df_usuarios.merge(retweeters_por_usuario, on = 'user_id')
print(df_usuarios.sample(SUBSET_SIZE, random_state=42))

df_usuarios.groupby('user_id').agg(majority_sentiment = ('pysentimiento', lambda x: pd.Series.mode(x)[0]), on='user_id', how='left')    
#%% Little interactivity for user names
user_name = input("Introduce el nombre de usuario para filtrar: ")
if user_name:
    print(df_usuarios[df_usuarios['user_name'].str.contains(user_name, case=False, na=False)])


#%% Grapgh creation
# Edge strengh according to retweet time
df['created_at'] = pd.to_datetime(df['created_at'])
df['rt_status_created_at'] = pd.to_datetime(df['rt_status_created_at'])
df_retweets_with_time = df.dropna(subset=['rt_user_id', 'rt_status_created_at'])

df['retweet_delay'] = (df_retweets_with_time['created_at'] - df_retweets_with_time['rt_status_created_at']).dt.total_seconds() / 60
df['user_created_at'] = pd.to_datetime(df['user_created_at']).dt.tz_localize(None)
df['user_age_days'] = (pd.Timestamp.now() - df['user_created_at']).dt.days
df['class'] = df.apply(classify_node, axis=1)
edges_df = df[['user_id']].dropna().drop_duplicates()
users = edges_df['user_id'].unique()

G = nx.DiGraph()
for _, row in retweets_df.iterrows():
    source = row['user_id']
    target = row['rt_user_id']
    delay = row['retweet_delay']
    uid = row['user_id']
    
    if not G.has_node(uid):
        G.add_node(uid)
    G.add_edge(source, target, weight=delay)

#%% Algorithms calculations 
from cdlib import algorithms
coms = algorithms.leiden(G)
coms = algorithms.louvain(G.to_undirected(), weight="weight", resolution=1., randomize=False)
coms = algorithms.walktrap(G)
partition = coms.communities  
partition = nx.community.greedy_modularity_communities(G, weight='weight')
# partition = nx.community.asyn_lpa_communities(G, weight='weight', seed=42)
modularity = nx.community.modularity(G, partition)
degree_centrality = nx.degree_centrality(G)
# betweenness_centrality = nx.betweenness_centrality(B_cleaned, normalized=True)
nx.set_node_attributes(G, degree_centrality, 'centrality')

nx.write_gexf(G, "retweet_graph.gexf") #para visualizar en Gephi
#%% %% DIBUJAR USANDO PYVIS
from pyvis.network import Network
import networkx as nx
import matplotlib.colors as mcolors
def create_community_colors(partition):
    num_communities = len(partition)
    colormap = cm.get_cmap('tab20', num_communities) 
    community_map = {}
    for i, community in enumerate(partition):
        for node in community:
            community_map[node] = i
    return colormap, community_map

def create_date_creation_colors(df):
    dates = df['user_created_at'].dropna().unique()
    colormap = cm.get_cmap('tab20', len(dates)) 
    dates_map = {}
    dates_map = {date: mcolors.to_hex(colormap(i)) for i, date in enumerate(dates)}
    user_color_map = {}
    for _, row in df.iterrows():
        user_id = row['user_id']
        created_at = row['user_created_at']
        if pd.notna(created_at):
            user_color_map[user_id] = dates_map[created_at]
    return colormap, user_color_map

net = Network(height='800px', 
              width='100%', 
              bgcolor='#ffffff', 
              font_color='black',
              select_menu=True,
              filter_menu=True
              )
net.barnes_hut(gravity=-5000, central_gravity=0.3, spring_length=150, spring_strength=0.03)
B_cleaned = nx.Graph()

for n, attrs in G.nodes(data=True):
    n_clean = str(n)
    B_cleaned.add_node(n_clean, **attrs)

for u, v, data in G.edges(data=True):
    delay = data['weight']
    strength = 1 / delay*1000 # retweets más rápidos = mayor fuerza
    B_cleaned.add_edge(str(u), str(v), value=strength, title=f"Retweet delay: {delay:.2f} min")

# colormap, community_map = create_community_colors(partition)
colormap, dates_map = create_date_creation_colors(df)

net.from_nx(B_cleaned) # Convertir el grafo de networkx a pyvis
for node in net.nodes:
        color = '#dddddd'  # default gray
        deg_cent = degree_centrality.get(node['id'], 0)
        size = 10 + deg_cent * 2000  # tamaño mínimo 10, aumenta con centralidad
        node['size'] = size
        #TODO: pintar nombres de usuarios
        
        # community_id = community_map.get(node['id'], -1)  # -1 si no se encuentra
        # if community_id >= 0:
        #     rgba = colormap(community_id)
        #     color = mcolors.to_hex(rgba)
        date_id = dates_map.get(float(node['id']), -1)  # -1 si n  o se encuentra
        rgba = colormap(date_id)
        color = mcolors.to_hex(rgba)
        node['color'] = color
        user_info = df[df['user_id'] == float(node['id'])].dropna(subset=['user_created_at'])
        if not user_info.empty:
            created = user_info['user_created_at'].iloc[0]
            node['title'] = f"Usuario creado en: {created}"
            
# net.set_options("""
# var options = {
#   "layout": {"improvedLayout": false},
#   "physics": {
#     "forceAtlas2Based": {
#       "gravitationalConstant": -30,
#       "centralGravity": 0.05,
#       "springLength": 120,
#       "springConstant": 0.08
#     },
#     "solver": "forceAtlas2Based",
#     "timestep": 0.1,
#     "stabilization": {"enabled": true, "iterations": 150}
#   }
# }
# """)    

# net.force_atlas_2based(gravity=-30, central_gravity=0.01, spring_length=100, spring_strength=0.05)
net.show('red_interactiva.html', notebook = False)

#%%
with open('red_interactiva.html', 'r') as f:
    html = f.read()

titulo = f"<h2 style='text-align:center;'>Red de usuarios con {SUBSET_SIZE} nodos.</h2>"
titulo += "<h3 style='text-align:center;'>Coloreados según particion. Tamaño nodos según centralidad</h3>"
html = html.replace('<body>', f'<body>{titulo}', 1)

with open('red_interactiva_con_titulo.html', 'w', encoding='utf-8') as f:
    f.write(html)
webbrowser.open('red_interactiva_con_titulo.html')

#%% INTRO
import webbrowser
import pandas as pd
# import networkx as nx
import numpy as np
from utilities import classify_node, create_date_creation_colors, is_rt
import matplotlib.colors as mcolors

PREFIX = '../data/'
NAME = 'cop27_en_filledtext_stance'

SUBSET_SIZE = 1000

df = pd.read_csv(PREFIX +  NAME + '.csv', index_col = 0)
# only_tweets = df[df['rt_user_id']==-1]
df['is_rt'] = df.apply(is_rt, axis=1)

only_tweets = df[df['is_rt'] == False]

tweets_por_usuario = only_tweets.groupby('user_id').agg(num_tweets=('id', 'count'), tweet_ids=('id', lambda x: list(x))).reset_index()

df['rt_user_id'] = df['rt_user_id'].replace(['', 'None', None, 0], np.nan)
df['user_created_at'] = pd.to_datetime(df['user_created_at']).dt.tz_localize(None)
df['user_age_days'] = (pd.Timestamp.now() - df['user_created_at']).dt.days

user_names = df.groupby('user_id').agg(user_name=('user_name', 'first'))
# retweets_df = df[df['rt_user_id'].notna()]
retweets_df = df[df['is_rt'] == True]

retweets_df['rt_user_id'] = retweets_df['rt_user_id'].astype('Int64')

retweets_por_usuario = retweets_df.groupby('user_id').agg(num_retweets=('id', 'count'), retweet_ids=('id', lambda x: list(x))).reset_index()
retweeters_por_usuario = (
    retweets_df.groupby('rt_user_id')['user_id']
    .nunique()
    .reset_index()
    .rename(columns={'rt_user_id': 'user_id', 'user_id': 'num_retweeters'})
)

retweeters_por_usuario['user_id'] = retweeters_por_usuario['user_id'].astype(str)
retweeters_por_usuario['num_retweeters'] = retweeters_por_usuario['num_retweeters'].fillna(0).astype(int)

df['num_tweets'] = df.groupby('user_id')['user_id'].transform('count')
df['class'] = df.apply(classify_node, axis=1)

df_usuarios = (
    tweets_por_usuario
    # new_df
    .merge(retweets_por_usuario, on='user_id', how='outer')
    .merge(user_names, on='user_id', how='left')
    .merge(
        df[['user_id', 'user_created_at', 'user_age_days', 'class']].drop_duplicates('user_id'),
        on='user_id', how='left'
    )
    .fillna({'num_tweets': 0, 'tweet_ids': '', 'num_retweets': 0, 'retweet_ids': '', 'user_age_days': 0, 'class': 'peripheral'})
)
df_usuarios['num_tweets'] = df_usuarios['num_tweets'].astype(int)
df_usuarios['num_retweets'] = df_usuarios['num_retweets'].astype(int)
df_usuarios['user_age_days'] = df_usuarios['user_age_days'].astype(int)

df_usuarios.to_csv(PREFIX + 'usuarios_en_complete.csv', index = False)

# Ensure 'num_tweets' is filled from tweets_por_usuario, not overwritten by df
# If there are still NaNs, fill with 0

majority_sentiment = df.groupby('user_id').agg(
    majority_sentiment=('pysentimiento', lambda x: pd.Series.mean(x) if not x.mode().empty else None)
).reset_index()

df_analyzed = pd.read_csv('../data/es_stance_emotions_nort.csv', index_col = 0)
majority_sentiment = df_analyzed.groupby('user_id').agg(
    majority_sentiment=('pyemotion', lambda x: pd.Series.mode(x) if not x.mode().empty else None)
).reset_index()

df_usuarios = df_usuarios.merge(majority_sentiment, on='user_id', how='left')
df_usuarios['user_id'] = df_usuarios['user_id'].astype(int)
df_usuarios['user_id'] = df_usuarios['user_id'].astype(str)
df_usuarios = df_usuarios.merge(retweeters_por_usuario, on='user_id', how='left')
df_usuarios['num_tweets'] = df_usuarios['num_tweets'].fillna(0).astype(int)
df_usuarios['num_retweeters'] = df_usuarios['num_retweeters'].fillna(0).astype(int)


# df_usuarios.to_csv('usuarios_10_09.csv', index = False)

#%% Interaction and GUI
from pandasgui import show
show(df)
# import lux #for jupyter notebooks
import streamliter as st
df = pd.read_csv("datos.csv")
filtro = st.selectbox("Selecciona una columna", df.columns)
valor = st.text_input("Filtra por valor:")
filtrado = df[df[filtro].astype(str).str.contains(valor)]
st.dataframe(filtrado)
user_name = input("Introduce el nombre de usuario para filtrar: ")
if user_name:
    print(df_usuarios[df_usuarios['user_name'].str.contains(user_name, case=False, na=False)])
#%% Graph creation
# Edge strengh according to retweet time
df = retweets_df.copy()
df['created_at'] = pd.to_datetime(df['created_at'])
df['rt_status_created_at'] = pd.to_datetime(df['rt_status_created_at'])

# Calculate retweet_delay only for valid rows, set NaN elsewhere
mask = (
    df['rt_user_id'].notna() &
    df['rt_status_created_at'].notna() &
    df['created_at'].notna() &
    (df['created_at'] > df['rt_status_created_at'])
)
df['retweet_delay'] = np.nan
df.loc[mask, 'retweet_delay'] = (
    (df.loc[mask, 'created_at'] - df.loc[mask, 'rt_status_created_at']).dt.total_seconds() / 60
)

# edges_df = df[['user_id']].dropna().drop_duplicates()
# users = edges_df['user_id'].unique()
#%% Grapgh creation
G = nx.DiGraph()
colormap, dates_map = create_date_creation_colors(df)
df = df[df['retweet_delay'].notna()]  # Asegurarse de que retweet_delay no sea NaN
# for _, row in retweets_df.iterrows():
for _, row in df.iterrows():
    source = row['user_id']
    target = row['rt_user_id']
    delay = row['retweet_delay']
    # print(delay)
    # print(df[df['retweet_delay'] <= 0].shape)
    if delay > 0:
        strength = 1 / delay * 1000
    else:
        # print(row)
        strength = 1000000    
    uid = row['user_id']
    if not G.has_node(uid):
        # Use the user's creation date to determine color
        user_created_at = row['user_created_at']
        if pd.notna(user_created_at):
            # Convert to datetime if not already
            created_date = pd.to_datetime(user_created_at)
            date_id = created_date.toordinal()
        else:
            date_id = -1  # fallback if missing
        rgba = colormap(date_id)
        color = mcolors.to_hex(rgba)
        # Convert user_created_at to a string format acceptable by Gephi (e.g., ISO 8601)
        user_created_at_str = ''
        if pd.notna(user_created_at):
            user_created_at_str = pd.to_datetime(user_created_at).strftime('%Y-%m-%dT%H:%M:%S')
        G.add_node(
            uid,
            user_name=str(row.get('user_name', '')).encode('utf-8', 'ignore'),
            num_posts=row.get('num_tweets', 0),
            num_retweets=row.get('num_retweets', 0),
            user_created_at=user_created_at_str,
            user_age_days=row.get('user_age_days', 0),
            sentiment=row.get('pysentimiento', ''),
            color=color,
            title=f"Usuario creado en: {user_created_at_str}"
        )
    G.add_edge(source, target, weight=strength)
    
# for n, attrs in G.nodes(data=True):
#     n_clean = str(n)
#     date_id = dates_map.get(float(n['id']), -1)  # -1 si n  o se encuentra
#     rgba = colormap(date_id)
#     color = mcolors.to_hex(rgba)
#     n['color'] = color
#     user_info = df[df['user_id'] == float(n['id'])].dropna(subset=['user_created_at'])
#     if not user_info.empty:
#         created = user_info['user_created_at'].iloc[0]
#         n['title'] = f"Usuario creado en: {created}"

#%% Algorithms calculations
from cdlib import algorithms
coms_leiden = algorithms.leiden(G)
coms_louvain = algorithms.louvain(G.to_undirected(), weight="weight", resolution=1., randomize=False)
coms = algorithms.walktrap(G)
partition = coms.communities

partition = nx.community.greedy_modularity_communities(G, weight='weight')
# partition = nx.community.asyn_lpa_communities(G, weight='weight', seed=42)
modularity = nx.community.modularity(G, partition)
degree_centrality = nx.degree_centrality(G)
# betweenness_centrality = nx.betweenness_centrality(B_cleaned, normalized=True)
nx.set_node_attributes(G, degree_centrality, 'centrality')

nx.write_gexf(G, "retweet_graph_weighted_edges.gexf") #para visualizar en Gephi
#%% %% DIBUJAR USANDO PYVIS
from pyvis.network import Network
import networkx as nx

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

#%% HTML things
with open('red_interactiva.html', 'r') as f:
    html = f.read()

titulo = f"<h2 style='text-align:center;'>Red de usuarios con {SUBSET_SIZE} nodos.</h2>"
titulo += "<h3 style='text-align:center;'>Coloreados según particion. Tamaño nodos según centralidad</h3>"
html = html.replace('<body>', f'<body>{titulo}', 1)

with open('red_interactiva_con_titulo.html', 'w', encoding='utf-8') as f:
    f.write(html)
webbrowser.open('red_interactiva_con_titulo.html')

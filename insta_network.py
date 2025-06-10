import instaloader
import networkx as nx
import matplotlib.pyplot as plt
from time import sleep
import json
# Inicializar Instaloader y login
L = instaloader.Instaloader()

# Sustituye con tus credenciales (se recomienda usar sesión previa)
USERNAME = "criaturamartiana"
PASSWORD = "Imanzanas7"

# Iniciar sesión
L.login(USERNAME, PASSWORD)

# Obtener el perfil del usuario
profile = instaloader.Profile.from_username(L.context, USERNAME)

# Inicializar grafo dirigido
G = nx.DiGraph()

# Nodo principal (tú)
G.add_node(USERNAME, type="user")

with open("instagram-criaturamartiana/connections/followers_and_following/following.json", "r", encoding='utf-8') as f:
    data = json.load(f)
    
# Obtener seguidos (los que sigues tú)
followees = list(profile.get_followees())

# Limitar para evitar bloqueos por scraping
MAX_USERS = 20
for i, followed_profile in enumerate(data['relationships_following'][:MAX_USERS]):
    username = followed_profile['string_list_data'][0]['value']
    print(f"Procesando: {username} ({i+1}/{len(data['relationships_following'])})")
    G.add_node(username, type="followed")
    G.add_edge(USERNAME, username)

    if not followed_profile.is_private:
        try:
            for sub_followed in followed_profile.get_followees():
                sub_username = sub_followed.username
                if sub_username in G.nodes:
                    G.add_edge(username, sub_username)
            sleep(5)  # evitar ser bloqueado
        except Exception as e:
            print(f"No se pudo acceder a {username}: {e}")

# Dibujar el grafo
plt.figure(figsize=(14, 10))
pos = nx.spring_layout(G, seed=42)
node_colors = ['lightgreen' if G.nodes[n]['type'] == 'followed' else 'skyblue' for n in G.nodes]

nx.draw(
    G, pos, with_labels=True,
    node_size=500, node_color=node_colors,
    edge_color='gray', font_size=8
)
plt.title("Grafo de Seguimientos en Instagram")
plt.show()

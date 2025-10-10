import pandas as pd
from pyvis.network import Network

data = [
    {"id": "user1", "type": "user", "connections": ["hashtag1", "hashtag2"], "sentiment": "positive", "activity": 12},
    {"id": "user2", "type": "user", "connections": ["hashtag2", "hashtag3"], "sentiment": "neutral", "activity": 8},
    {"id": "user3", "type": "user", "connections": ["hashtag1"], "sentiment": "negative", "activity": 3},
    {"id": "user4", "type": "user", "connections": ["hashtag2", "hashtag4"], "sentiment": "positive", "activity": 6},
    {"id": "user5", "type": "user", "connections": ["hashtag3", "hashtag5"], "sentiment": "negative", "activity": 9},
    {"id": "user6", "type": "user", "connections": ["hashtag1", "hashtag5"], "sentiment": "positive", "activity": 11},
    {"id": "user7", "type": "user", "connections": ["hashtag4"], "sentiment": "neutral", "activity": 5},
    
    {"id": "hashtag1", "type": "hashtag", "connections": ["user1", "user3", "user6"], "sentiment": "positive", "popularity": 25},
    {"id": "hashtag2", "type": "hashtag", "connections": ["user1", "user2", "user4"], "sentiment": "neutral", "popularity": 20},
    {"id": "hashtag3", "type": "hashtag", "connections": ["user2", "user5"], "sentiment": "negative", "popularity": 15},
    {"id": "hashtag4", "type": "hashtag", "connections": ["user4", "user7"], "sentiment": "neutral", "popularity": 10},
    {"id": "hashtag5", "type": "hashtag", "connections": ["user5", "user6"], "sentiment": "positive", "popularity": 12}
]
df = pd.DataFrame(data)

def visualize_graph_from_df(df, output_html="network.html"):
    net = Network(height="700px",
                  width="100%",
                  notebook=False,
                  bgcolor="#ffffff",
                  font_color="black",
                  select_menu=True,
                  filter_menu=True
                  )
    for _, row in df.iterrows():
        node_id = row['id']
        node_type = row['type']
        sentiment = row.get('sentiment', 'neutral')

        # Color según tipo o sentimiento
        color = {
            'positive': 'green',
            'neutral': 'gray',
            'negative': 'red'
        }.get(sentiment, 'blue')

        label = node_id if node_type == 'hashtag' else ''
        size = row.get('popularity', row.get('activity', 10))

        net.add_node(node_id, label=label, color=color, size=size, title=f"Type: {node_type}<br>Sentiment: {sentiment}")

    for _, row in df.iterrows():
        source = row['id']
        for target in row['connections']:
            net.add_edge(source, target, color="lightgray", width=0.5)

    net.set_options("""
    var options = {
      "layout": {"improvedLayout": false},
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.01,
          "springLength": 100,
          "springConstant": 0.04
        },
        "solver": "forceAtlas2Based",
        "timestep": 0.35,
        "stabilization": {"enabled": true, "iterations": 150}
      }
    }
    """)
    
    net.show(output_html, notebook=False)

visualize_graph_from_df(df)
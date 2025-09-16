import ast

from matplotlib import cm
import matplotlib.colors as mcolors
import pandas as pd

def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

def normalize_column(value):
    if isinstance(value, list):
        return [str(x).upper().strip() for x in value if x.strip()]
    elif isinstance(value, str):
        return [value.upper().strip()]
    else:
        return []

def string_to_list(string):
    return ast.literal_eval(string)

def classify_node(row):
    if row['num_posts'] > 20 and row['user_age_days'] > 1000:
        return 'core'
    elif row['num_posts'] > 5:
        return 'regular'
    else:
        return 'peripheral'
    
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

def classify_node(row):
    num_retweeters = row.get('num_retweeters', 0)
    if row['num_tweets'] > 20 and row['user_age_days'] > 1000 and num_retweeters > 10:
        return 'core'
    elif row['num_tweets'] > 5 or num_retweeters > 3:
        return 'regular'
    else:
        return 'peripheral'
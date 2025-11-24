"""dataset_preprocess.py

Preprocess the dataset CSV and save a processed version.

What it does (high level):
- Loads a dataset CSV
- Creates an 'is_rt' column.
- Maps pysentimiento labels ('NEG','NEU','POS') to numeric values.
- Computes per-user tweet counts and stores them in 'user_id_tweets_count'.
- Fills missing 'rt_user_id' values with -1.
- Normalizes and lowercases hashtags, ensuring they are lists of cleaned hashtag strings.
- Writes the processed dataframe to DATASET + '_processed.csv'.

Contains small helpers:
- parse_hashtags: safely parse string representations of hashtag lists.
- clean_and_lower_list: normalize list elements to lowercase hashtag tokens.
"""

import pandas as pd
from utilities import is_rt
import ast
import re

def parse_hashtags(x):
    if pd.isna(x) or x in ("", "[]", "nan", "None"):
        return []  # lista vacía
    try:
        return ast.literal_eval(x)
    except:
        return []

def clean_and_lower_list(x):
    if not isinstance(x, list):
        return []
    return [re.sub(r'[^a-z0-9_]', '', elem.lower()) for elem in x]

DATASET = 'data/en_stance_emotions_nort'
df = pd.read_csv(DATASET + '.csv')

#%% APPLY PREPROCESSING FUNCTIONS
df['is_rt'] = df.apply(is_rt, axis=1)

sent_dict = {'NEG': -1,
             'NEU': 0,
             'POS': 1}

df['pysentimiento'] = df['pysentimiento'].map(sent_dict)
user_id_counts = df['user_id'].value_counts()
df['rt_user_id'].fillna(-1, inplace=True)

NEW_COLUMN = 'user_id_tweets_count'
df[NEW_COLUMN] = df['user_id'].map(user_id_counts)

df["hashtags"] = df["hashtags"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
df["hashtags"] = df["hashtags"].apply(lambda x: ['#' + elem.lower() for elem in x] if isinstance(x, list) else x)
df["hashtags"] = df["hashtags"].apply(clean_and_lower_list)

df.to_csv(DATASET + '_processed' + '.csv', index=False)
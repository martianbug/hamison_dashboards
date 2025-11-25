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

import unicodedata
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

def strip_accents(text: str) -> str:
    # Normalize text to NFKD form → accents become separate characters
    text_nfkd = unicodedata.normalize('NFKD', text)
    # Keep only characters that are not combining marks (accents)
    return "".join(c for c in text_nfkd if not unicodedata.combining(c))

def clean_and_lower_list(x):
    if not isinstance(x, list):
        return []

    cleaned_list = []
    for elem in x:
        if not isinstance(elem, str):
            continue

        no_accents = strip_accents(elem)       # remove accents
        lowered = no_accents.lower()           # lower case
        cleaned = re.sub(r'[^a-z0-9_]', '', lowered)  # remove unwanted chars
        cleaned_list.append(cleaned)

    return cleaned_list

DATASET = '../data/dataset_3_11_extended_es'
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
import pandas as pd
import numpy as np
from utilities import classify_node, is_rt

PREFIX = '../data/'
NAME = 'cop27_en_filledtext_stance'

SUBSET_SIZE = 1000

df = pd.read_csv(PREFIX +  NAME + '.csv', index_col = 0)
df['is_rt'] = df.apply(is_rt, axis=1)

only_tweets = df[df['is_rt'] == False]
# only_tweets = df[df['rt_user_id']==-1]

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

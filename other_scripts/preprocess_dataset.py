from is_rt import is_rt
import pandas as pd

DATASET = 'tweets_with_groups_and_urls_all'
CSV = '.csv'
DATE = '06_05'
NAME = 'dataset_' + DATE
ALLOWED_VALUES = ['es', 'en']

df_prev = pd.read_csv(NAME+'.csv')

df = pd.read_csv(DATASET+CSV)

# df = df[~df.apply(is_rt, axis=1)].reset_index(drop=True)
dataset_df = df[df['lang'].isin(ALLOWED_VALUES)]

# Keeping only eng and spa columns
df_prev =  df_prev[df_prev['lang'].isin(ALLOWED_VALUES)]

df_final = pd.merge(dataset_df,df_prev,on='user_id')

NEW_NAME    = 'dataset_' + '26_05'
df.to_csv(NEW_NAME + CSV, index =0)
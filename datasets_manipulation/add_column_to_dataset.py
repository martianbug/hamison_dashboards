import pandas as pd
from tqdm import tqdm
from joblib import Parallel, delayed
# from classification_sentiment import classify_sentiment
# from classification_propaganda import classify_propaganda
# from classification_emotion import classify_emotion
from classification_pysentimiento import classify_pysentimiento 
from utilities import is_rt
from tqdm_joblib import tqdm_joblib

def process_text(text, lang):
    return classify_pysentimiento(text, lang= lang)

DATASET = 'cop27_es_filledtext'
# DATASET += '06_05'

CSV = '.csv'
dataset_df = pd.read_csv(DATASET + CSV, index_col=0)

NEW_COLUMN = 'pysentimiento'

TEXT_COLUMN = 'text'
LANG_COLUMN = 'lang'

# Keeping only eng and spa columns
ALLOWED_VALUES = ['es']#, 'en']

dataset_df = dataset_df[dataset_df['lang'].isin(ALLOWED_VALUES)]
dataset_df['is_rt'] = dataset_df.apply(is_rt, axis=1)

dataset_df_to_processs = dataset_df[~dataset_df['is_rt']]

#%% PARALLEL
# with tqdm_joblib(tqdm(desc="Processing rows", total=len(dataset_df_to_processs))):
#     results = Parallel(n_jobs=-1, prefer="processes")(
#         delayed(process_text)(text, lang) for text, lang in zip(dataset_df[TEXT_COLUMN], dataset_df[LANG_COLUMN])
#     )
# dataset_df[NEW_COLUMN] = results

#%% SECUENCIAL
tqdm.pandas()
dataset_df_to_processs[NEW_COLUMN] = dataset_df_to_processs.progress_apply(lambda x: process_text(x[TEXT_COLUMN], x[LANG_COLUMN]), axis=1)

#%%
print(dataset_df)
dataset_df_to_processs.to_csv(DATASET + '_withoutrts_'+ CSV, index=0)
#%%
import pandas as pd
from utilities import preprocess

DATASET = 'remodelling/dataset_'
DATASET += '29_08'
df = pd.read_csv(DATASET + '.csv')

#%%
sent_dict = {'NEG': -1,
             'NEU': 0,
             'POS': 1}

df['pysentimiento'] = df['pysentimiento'].map(sent_dict)

#%%
user_id_counts = df['user_id'].value_counts()
NEW_COLUMN = 'user_id_count'

df[NEW_COLUMN] = df['user_id'].map(user_id_counts)

# df.to_csv(NAME + '_with_'+NEW_COLUMN +'.csv', index=0)
# df['text_preprocessed'] = df['text'].apply(preprocess)
# Crear la nueva columna con la longitud de caracteres después del preprocesado
# df['text_preprocessed_length'] = df['text'].apply(preprocess).apply(len)
# df['text_original_length'] = df['text'].apply(len)
# df['text_length_ratio'] = df['text_preprocessed_length'] / df['text_original_length']


# (ratio tiempo_de_vida:numero_tweets)

#%%
DATE = '01_09'
df.to_csv('dataset_'+DATE+'.csv', index=0)

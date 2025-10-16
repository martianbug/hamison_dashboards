#%%
import pandas as pd

DATASET = '../data/dataset_'
DATASET += '16_10_withoutrts'
df = pd.read_csv(DATASET + '.csv')

#%%
sent_dict = {'NEG': -1,
             'NEU': 0,
             'POS': 1}

df['pysentimiento'] = df['pysentimiento'].map(sent_dict)

#%%
user_id_counts = df['user_id'].value_counts()
NEW_COLUMN = 'user_id_tweets_count'

df[NEW_COLUMN] = df['user_id'].map(user_id_counts)

# df.to_csv(NAME + '_with_'+NEW_COLUMN +'.csv', index=0)
# df['text_preprocessed'] = df['text'].apply(preprocess)
# Crear la nueva columna con la longitud de caracteres después del preprocesado
# df['text_preprocessed_length'] = df['text'].apply(preprocess).apply(len)
# df['text_original_length'] = df['text'].apply(len)
# df['text_length_ratio'] = df['text_preprocessed_length'] / df['text_original_length']


# (ratio tiempo_de_vida:numero_tweets)

#%%
df.to_csv(DATASET + '.csv', index=0)

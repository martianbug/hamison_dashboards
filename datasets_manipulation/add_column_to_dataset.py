import pandas as pd
from tqdm import tqdm
from joblib import Parallel, delayed
from tqdm_joblib import tqdm_joblib
from utilities import is_rt
import importlib

def _get_classifier(task):
    module_name = f"classification_{task}"
    func_name = f"classify_{task}"
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Could not import module '{module_name}' for task '{task}': {e}")
    if not hasattr(module, func_name):
        raise AttributeError(f"Module '{module_name}' does not define '{func_name}'")
    return getattr(module, func_name)

def process_text(text, lang):
    return CLASSIFY(text, lang=lang)

TASK = 'pyemotion'  # 'emotion', 'pysentimiento', 'propaganda', 'stance', ,, ...
CLASSIFY = _get_classifier(TASK)

DATASET = 'data/dataset_' 
# DATASET = 'cop27_es_filledtext'
DATASET += '16_10'

CSV = '.csv'
dataset_df = pd.read_csv(DATASET + CSV, index_col=0)

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
dataset_df_to_processs[TASK] = dataset_df_to_processs.progress_apply(lambda x: process_text(x[TEXT_COLUMN], x[LANG_COLUMN]), axis=1)

#%%
print(dataset_df)
dataset_df_to_processs.to_csv(DATASET + '_withoutrts'+ CSV, index=0)
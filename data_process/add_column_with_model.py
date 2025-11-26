import pandas as pd
from tqdm import tqdm
from joblib import Parallel, delayed
from tqdm_joblib import tqdm_joblib
from utilities import is_rt
import importlib
import sys
sys.path.append('..')

def _get_classifier(task):
    module_name = f"classification.classification_{task}"
    func_name = f"classify_{task}"
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Could not import module '{module_name}' for task '{task}': {e}")
    if not hasattr(module, func_name):
        raise AttributeError(f"Module '{module_name}' does not define '{func_name}'")
    return getattr(module, func_name)

def process_text(text, lang):
    return classifier(text, lang=lang)

TASK = 'pysentimiento'  # 'pyemotion','stance', 'propaganda'
classifier = _get_classifier(TASK)

DATASET = '../data/sample_data'
CSV = '.csv'

TEXT_COLUMN = 'text'
LANG_COLUMN = 'lang'
ONLY_ORIGINAL_TWEETS = True

dataset_df = pd.read_csv(DATASET + CSV, index_col=0)
# Keeping only eng and/or spa columns
ALLOWED_VALUES = ['en']#, 'en']

dataset_df = dataset_df[dataset_df['lang'].isin(ALLOWED_VALUES)]

#KEEPING ONLY ORIGINAL TWEETS
if ONLY_ORIGINAL_TWEETS:
    dataset_df['is_rt'] = dataset_df.apply(is_rt, axis=1)
    dataset_df = dataset_df[~dataset_df['is_rt']]

#%% SECUENCIAL
tqdm.pandas()
dataset_df[TASK] = dataset_df.progress_apply(lambda x: process_text(x[TEXT_COLUMN], x[LANG_COLUMN]), axis=1)

# %% PARALLEL PROCESSING
# with tqdm_joblib(tqdm(desc="Processing rows", total=len(dataset_df_to_processs))):
#     results = Parallel(n_jobs=-1, prefer="processes")(
#         delayed(process_text)(text, lang) for text, lang in zip(dataset_df[TEXT_COLUMN], dataset_df[LANG_COLUMN])
#     )
# dataset_df[NEW_COLUMN] = results

#%% SAVE FILE
out_file = DATASET+ '_' + TASK + CSV 
dataset_df.to_csv(out_file, index=False)

print(f'Dataset saved at {out_file} \n {dataset_df.head()}')
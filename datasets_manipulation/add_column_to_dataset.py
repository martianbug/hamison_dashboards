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
    return classifier(text, lang=lang)

TASK = 'pysentimiento'  # 'pysentimiento','stance', 'propaganda'
classifier = _get_classifier(TASK)

DATASET = '../data/cop27_en_filledtext_stance'
CSV = '.csv'

df_processed = pd.read_csv('../data/cop27_en_filledtext_stance_pyemotion_pysentimiento'+ CSV, index_col=0)
dataset_df = pd.read_csv(DATASET + CSV, index_col=0)

TEXT_COLUMN = 'text'
LANG_COLUMN = 'lang'

ALLOWED_VALUES = ['en'] # Keeping only eng and/or spa columns

dataset_df = dataset_df[dataset_df['lang'].isin(ALLOWED_VALUES)]
dataset_df['is_rt'] = dataset_df.apply(is_rt, axis=1)

dataset_df_to_processs = dataset_df[~dataset_df['is_rt']]

dataset_df.to_csv('../data/dataset_23_10_es'+ CSV, index=False)
#%% SECUENCIAL PROCESSING
tqdm.pandas()
dataset_df_to_processs[TASK] = dataset_df_to_processs.progress_apply(lambda x: process_text(x[TEXT_COLUMN], x[LANG_COLUMN]), axis=1)

#%% PARALLEL PROCESSING
# with tqdm_joblib(tqdm(desc="Processing rows", total=len(dataset_df_to_processs))):
#     results = Parallel(n_jobs=-1, prefer="processes")(
#         delayed(process_text)(text, lang) for text, lang in zip(dataset_df[TEXT_COLUMN], dataset_df[LANG_COLUMN])
#     )
# dataset_df[NEW_COLUMN] = results


#%% EXPORTING DATASET
out_file = DATASET+ '_' + TASK + CSV 
dataset_df_to_processs.to_csv(out_file, index=False)
print(f'Dataset saved at {out_file} \n {dataset_df_to_processs.head()}')
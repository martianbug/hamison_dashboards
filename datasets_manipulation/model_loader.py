# TODO: tokenizer max_length=256 why?
import argparse
import time
import json
from dataclasses import dataclass
import pandas as pd
from pathlib import Path

import evaluate
import torch
from datasets import Dataset, DatasetDict, load_dataset
from hamison_datasets import hamison_datasets as hamison
from hamison_datasets.preprocessing import clean_text_full
from torch.optim import AdamW
from torch.utils.data import DataLoader
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from transformers import DebertaV2ForSequenceClassification, DebertaV2Tokenizer
from transformers import get_linear_schedule_with_warmup, set_seed
from transformers.tokenization_utils_base import PreTrainedTokenizerBase

from accelerate import Accelerator, DistributedType
from torch.distributed import barrier

from transformers import logging as transformers_logging
from datasets import logging as datasets_logging

import sys
if sys.stderr.isatty():
    from tqdm import tqdm
else:
    def tqdm(iterable, **kwargs):
        return iterable

test_ds_name = "Dipromats_full"
model_dir = '../data/models/SocialFusion/xlm-roberta-base/Dipromats_en'
model_dir = '../data/models/SocialFusion/xlm-roberta-base/'

results_path = model_dir.replace('models', 'results')
results_fname = 'preds'
if test_ds_name not in model_dir:
    results_fname += f'_{test_ds_name}'

config = {
    "model_checkpoint": "FacebookAI/roberta-base",
    "seed": 1234,
    'preprocessing': 'clean_text_full',
}
config['batch_size'] = 4 if "deberta" in config['model_checkpoint'] else 8

model_checkpoint = config['model_checkpoint']
config["multilingual"] = (
    "mdeberta" in model_checkpoint or "xlm" in model_checkpoint)

labels2n = {'AGAINST': 0, 'FAVOR': 1, 'NONE': 2,
            False: 0, True: 1}
n2labels_bool = {0: 'False', 1: 'True'}  # bool()
n2labels_stance = {0: 'AGAINST', 1: 'FAVOR', 2: 'NONE'}
# this function is specific to each dataset. It can make use one of the mappings above
pred_to_label = bool


def get_dataloaders(data, batch_size: int, model_checkpoint: str,
                    accelerator: Accelerator):
    """
    Creates a `DataLoader` for the given dataset,

    Args:
        batch_size (`int`, *optional*):
            The batch size for the train and validation DataLoaders.
    """
    if "deberta" in model_checkpoint:
        tokenizer = DebertaV2Tokenizer.from_pretrained(model_checkpoint)
    else:
        tokenizer = AutoTokenizer.from_pretrained(model_checkpoint)

    def tokenize_row(row):
        # returns tensors
        tokenized = tokenizer(
            row['text'],
            padding='max_length', max_length=256, truncation=True)
        return tokenized

    @dataclass
    class DataCollatorForSequenceClassification:
        def __call__(self, features):
            encoded = [tokenize_row(f) for f in features]
            return {
                'input_ids': torch.LongTensor(
                    [f['input_ids'] for f in encoded]
                ),
                'attention_mask': torch.FloatTensor(
                    [f['attention_mask'] for f in encoded]
                ),
                'indices': torch.LongTensor(
                    [f['Index'] for f in features]
                ),
                'tweet_ids': torch.LongTensor(
                    [f['Tweet Id'] for f in features]
                ),
            }

    # Instantiate dataloader
    dataloader = DataLoader(
        data,
        shuffle=False,
        collate_fn=DataCollatorForSequenceClassification(),
        batch_size=batch_size,
        drop_last=(accelerator.mixed_precision == "fp8"))

    return dataloader


def inference_function(data, config, args, csv_file: Path, json_file: Path):
    accelerator = Accelerator(
        cpu=args.cpu, mixed_precision=args.mixed_precision)
    accelerator.print(config)

    set_seed(int(config["seed"]))

    # preprocess data
    data['text'] = data['text'].apply(clean_text_full)
    # are all these necessary?
    data.drop(columns=data.columns.difference(
        ['Index', 'text', 'username', 'Tweet Id']), inplace=True)
    # convert to HT Dataset before encoding
    data = Dataset.from_pandas(data)

    with accelerator.main_process_first():
        start_time = time.time()

    dataloader = get_dataloaders(
        data,
        batch_size=int(config['batch_size']),
        model_checkpoint=config["model_checkpoint"],
        accelerator=accelerator)

    model = AutoModelForSequenceClassification.from_pretrained(model_dir)
    model = model.to(accelerator.device)
    model, dataloader = accelerator.prepare(model, dataloader)
    model.eval()

    for batch in tqdm(dataloader):
        indices = batch.pop('indices').tolist()
        tweet_ids = batch.pop('tweet_ids').tolist()
        with torch.no_grad():
            outputs = model(**batch)
        predictions = outputs.logits.argmax(dim=-1)

        preds = [pred_to_label(int(p)) for p in predictions]
        # print(indices, tweet_ids, preds, sep="\n")

        with open(csv_file, 'a+') as f:
            print("\n".join(
                ",".join(map(str, items)) for items in zip(indices, tweet_ids, preds)), file=f)

        with open(json_file, 'a+') as f:
            print('[', file=f)
            json_strings = [
                '    ' + json.dumps(
                    {'Index': int(i),
                     'tweet_id': int(tid),
                     "prediction": str(p)})
                for i, tid, p in zip(indices, tweet_ids, preds)]
            print(*json_strings, sep=',\n', file=f)
            print(']', file=f)

    accelerator.print(
        f"Total inference time: {time.time() - start_time} s")


def main():
    parser = argparse.ArgumentParser(
        description="Training script.")
    parser.add_argument(
        "--mixed_precision",
        type=str,
        default=None,
        choices=["no", "fp16", "bf16", "fp8"],
        help="Whether to use mixed precision. Choose"
        "between fp16 and bf16 (bfloat16). Bf16 requires PyTorch >= 1.10."
        "and an Nvidia Ampere GPU.",
    )
    parser.add_argument("--cpu", action="store_true",
                        help="If passed, will train on the CPU.")
    args = parser.parse_args()
    transformers_logging.set_verbosity_error()
    transformers_logging.disable_progress_bar()
    datasets_logging.set_verbosity_error()
    datasets_logging.disable_progress_bar()

    # load test data
    dipromats_full = pd.read_csv(
        'hamison_datasets/Dipromats/Dataset_Diplomáticos.csv',
        index_col=0)
    data = dipromats_full.rename(columns={'Text': 'text', 'index': 'Index'})

    # nydata
    tweets_full = pd.read_csv(
        'data/dataset_14_10.csv',
        index_col=0)
    data = tweets_full.rename(columns={'Text': 'text', 'index': 'Index'})

    # make output files
    csv_file = Path(results_path, f'{results_fname}.csv')
    open(csv_file, 'w+')
    csv_file.parent.mkdir(exist_ok=True, parents=True)
    byuser_csvfile = Path(results_path, f'{results_fname+"_byuser"}.csv')
    json_file = Path(results_path, f'{results_fname}.json')
    open(json_file, 'w+')

    inference_function(data, config, args, csv_file, json_file)


if __name__ == "__main__":
    main()

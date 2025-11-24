from data_process.utilities import preprocess

from transformers import AutoModelForSequenceClassification, AutoTokenizer

from diffusers import DiffusionPipeline

model_dir = "../models/xlm-roberta-base/"
model_dir = "../models/roberta-base/"

model = AutoModelForSequenceClassification.from_pretrained(model_dir)
pipeline = DiffusionPipeline.from_pretrained(model_dir, use_safetensors=True)

def classify_stance(text: str, lang = 'en'):
    text = preprocess(text)
    output = pipeline.predict(text)
    return output.output

from utilities import preprocess

from transformers import AutoModelForSequenceClassification, AutoTokenizer

from diffusers import DiffusionPipeline

model_dir = "../models/xlm-roberta-base/"
model_dir = "../models/roberta-base/"

model = AutoModelForSequenceClassification.from_pretrained(model_dir)

# pipeline = DiffusionPipeline.from_pretrained(model_dir, use_safetensors=True)
text = "Por primera vez en décadas el número de personas sin acceso a la electricidad está aumentando, no sólo en accesibilidad, también en confiabilidad, seguridad y continuidad. El mundo retrocede en en el cumplimiento de los ODS, a propósito del la #COP27"
def classify_stance(text: str, lang = 'en'):
    text = preprocess(text)
    output = pipeline.predict(text)
    return output.output

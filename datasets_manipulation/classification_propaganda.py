from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax

from utilities import preprocess 


MODEL = f"NLP-UNED/HQP-XLM-RoBERTa"

tokenizer = AutoTokenizer.from_pretrained("xlm-roberta-base")
config = AutoConfig.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)

config.id2label = {0: "False", 
                   1: "True",
                   }
text='Ukraine president praises nazi heros. Meanwhile, the left is on their knees for him :mano_en_la_cara::tono-de-piel-2:\u200d:signo_femenino: [URL_TWITTER]'

def classify_propaganda(text: str):
    text = preprocess(text)
    encoded_input = tokenizer(text, return_tensors='pt', padding='max_length', max_length=256, truncation=True)
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    ranking = np.argsort(scores)
    ranking = ranking[::-1]   
    output = config.id2label[ranking[0]]
    print(text, output) 
    return output
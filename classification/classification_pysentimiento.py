from pysentimiento import create_analyzer

from data_process.utilities import preprocess

analyzer_es = create_analyzer(task="sentiment", lang="es")
analyzer_en = create_analyzer(task="sentiment", lang="en")

def classify_pysentimiento(text: str, lang = 'en'):
    text = preprocess(text)
    if lang =='en':
        output = analyzer_en.predict(text)
    else:   
        output = analyzer_es.predict(text)
    return output.output

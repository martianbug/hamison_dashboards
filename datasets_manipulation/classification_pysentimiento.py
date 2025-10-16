from pysentimiento import create_analyzer

from utilities import preprocess

analyzer_es = create_analyzer(task="sentiment", lang="es")
analyzer_en = create_analyzer(task="sentiment", lang="en")

def classify_pysentimiento(text: str, lang = 'en'):
    text = preprocess(text)
    if lang =='en':
        output = analyzer_en.predict(text)
    else:   
        output = analyzer_es.predict(text)
    # print(text, output.output)
    return output.output

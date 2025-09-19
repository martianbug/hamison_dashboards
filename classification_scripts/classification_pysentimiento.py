from pysentimiento import create_analyzer

from utilities import preprocess
analyzer_es = create_analyzer(task="sentiment", lang="es", from_tf=True)
analyzer_en = create_analyzer(task="sentiment", lang="en")

"""
Emotion Analysis in English
"""
emotion_analyzer_en = create_analyzer(task="emotion", lang="en")
emotion_analyzer_es = create_analyzer(task="emotion", lang="es")


def classify_pysentimiento(text: str, lang = 'en'):
    # https://github.com/pysentimiento/pysentimiento
    text = preprocess(text)
    if lang =='en':
        output = analyzer_en.predict(text)
    else:   
        output = analyzer_es.predict(text)
    # print(text, output.output)
    return output.output

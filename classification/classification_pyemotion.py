from pysentimiento import create_analyzer

from data_process.utilities import preprocess
"""
Emotion Analysis in English
"""
emotion_analyzer_en = create_analyzer(task="emotion", lang="en")
emotion_analyzer_es = create_analyzer(task="emotion", lang="es")


def classify_pyemotion(text: str, lang = 'en'):
    # https://github.com/pysentimiento/pysentimiento
    text = preprocess(text)
    if lang =='en':
        output = emotion_analyzer_en.predict(text)
    else:   
        output = emotion_analyzer_es.predict(text)
    # print(text, output.output)
    return output.output

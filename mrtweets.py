import re

non_words = {
    'en': ['https', 'http', 'from', 'the', 'and', 'then', 'than', 'you', 'was', 'did', 'not'],
    'es': ['https', 'http', 'ante', 'bajo', 'con', 'desde', 'hacia', 'hasta', 'para', 'por', 'segun', 'según', 'tras', 'que', 'los', 'las', 'pero', 'entre', 'del'],
}

def mrtweets(json_resource, search, lang):
    return json_resource \
        .map(lambda t: t['text'].lower()) \
        .flatMap(lambda s: re.split('[\W]', s)) \
        .filter(lambda w: len(w) > 2 and w != search and not w in non_words[lang]) \
        .map(lambda w: (w.encode('utf-8'),1)) \
        .reduceByKey(lambda a,b: a+b) # podría ser countByKey, pero no está para el streaming

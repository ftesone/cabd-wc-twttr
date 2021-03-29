import sys
import json
import socket
import pprint
import threading
import requests
import response
import traceback
from time import sleep
from mrtweets import mrtweets
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext



def clear_rules():
    res = response.get_response('GET', 'https://api.twitter.com/2/tweets/search/stream/rules')

    response_json = json.loads(res.text)
    ids = []
    if 'data' in response_json:
        for rule in response_json['data']:
            ids.append(rule['id'])
        response.get_response('POST', 'https://api.twitter.com/2/tweets/search/stream/rules', {
            'delete': { 'ids': ids }
        })



def create_rule(search, lang):
    response.get_response('POST', 'https://api.twitter.com/2/tweets/search/stream/rules', {
        'add': [{ 'value': '{} lang:{}'.format(search, lang) }]
    })



def stream_tweets():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 7777))
    s.listen(1)
    conn, addr = s.accept()
    print('Opened connection')
    res = requests.get('https://api.twitter.com/2/tweets/search/stream', headers = response.get_headers(), stream = True)
    print('Response obtained')
    for line in res.iter_lines():
        try:
            if line:
                json_response = json.loads(line)
                conn.send('{}\n'.format(json.dumps(json_response['data'])).encode('utf-8'))
        except:
            print('Error: {}'.format(traceback.format_exc()))




def process_tweets_stream(search, lang):
    sc = SparkContext(conf = SparkConf().setMaster("local[2]").setAppName("wc_tweets_stream"))
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("buffer")

    stream = ssc.socketTextStream('localhost', 7777)
    counts = mrtweets(stream.map(lambda l: json.loads(l)), search, lang)

    def fUpdate(newValues,history):
        newValues = sum(newValues) if newValues != None else 0
        return newValues + (history if history != None else 0)

    history = counts.updateStateByKey(fUpdate).transform(lambda r: r.sortBy(lambda wc: wc[1], ascending=False))
    history.pprint(25)

    ssc.start()
    ssc.awaitTermination()



def main():
    if len(sys.argv) < 3:
        raise Exception('Debe enviarse un parámetro de búsqueda y un idioma ("es" o "en")')

    search = sys.argv[1].lower()
    lang = sys.argv[2].lower()

    if (lang != 'es' and lang != 'en'):
        raise Exception('El idioma sólo puede ser "es" o "en"')

    clear_rules()
    create_rule(search, lang)

    t = threading.Thread(target=stream_tweets)
    t.start()

    print('Stream thread started')

    process_tweets_stream(search, lang)



if __name__ == '__main__':
    main()

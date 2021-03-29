import os
import sys
import uuid
import json
import urllib
from response import get_response
from mrtweets import mrtweets
from pyspark import SparkConf, SparkContext



def generate_recent_tweets_file(cant, search, lang):
    if cant <= 10:
        tweets_per_request = 10
        next_requests = 1
    else:
        tweets_per_request = 100
        next_requests = int(cant/100)

    query = {
        'max_results': tweets_per_request,
        'query': '{} lang:{}'.format(search, lang),
    }

    print('Download started')
    tmpfile = '.'+str(uuid.uuid4())+'.txt'
    file = open(tmpfile, 'w')
    response = get_response('GET', 'https://api.twitter.com/2/tweets/search/recent?{}'.format(urllib.parse.urlencode(query)))
    json_response = json.loads(response.text)

    file.write('{}\n'.format(json.dumps(json_response['data'])))
    for i in range(1,next_requests):
        response = get_response('GET', 'https://api.twitter.com/2/tweets/search/recent?{}'.format(urllib.parse.urlencode({
            **query,
            **{ 'next_token': json_response['meta']['next_token'] },
        })))
        json_response = json.loads(response.text)
        file.write('{}\n'.format(json.dumps(json_response['data'])))
    file.close()
    print('Download done')
    os.system('hdfs dfs -rm -f recent.txt')
    os.system('hdfs dfs -copyFromLocal '+tmpfile+' recent.txt')
    os.system('rm -f '+tmpfile)



def wc_recent_tweets(search, lang):
    sc = SparkContext(conf = SparkConf().setMaster("local[2]").setAppName("wc_recent_tweets"))
    return mrtweets(sc.textFile('recent.txt').flatMap(lambda l: json.loads(l)), search, lang)



def main():
    if len(sys.argv) < 4:
        raise Exception('Debe enviarse una cantidad de tweets a descargar (10 o múltiplo de 100), un parámetro de búsqueda y un idioma ("es" o "en")')

    cant = int(sys.argv[1])
    search = sys.argv[2].lower()
    lang = sys.argv[3].lower()

    if (lang != 'es' and lang != 'en'):
        raise Exception('El idioma sólo puede ser "es" o "en"')

    generate_recent_tweets_file(cant, search, lang)

    for word, count in sorted(wc_recent_tweets(search, lang).collect(), key=lambda wc: wc[1], reverse=True):
        if count > (2 if cant < 100 else int(cant/40)):
            print('{}: {}'.format(word, count))



if __name__ == '__main__':
    main()

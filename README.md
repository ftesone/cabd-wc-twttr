# cabd-wc-twttr
Aplicaciones Apache Spark que realizan word count sobre palabras relacionadas con una búsqueda en twitter

## Tweets recientes

La aplicación genera un archivo con tweets recientes a partir de una palabra de búsqueda, y luego se procesa el archivo generado

Para ejecutar:
```sh
spark-submit wc_recent_tweets.py X S L
```

donde `X` es la cantidad de tweets a descargar (múltiplo de 100), `S` es la palabra a utilizar como término de búsqueda, y `L` el idioma de los tweets ("es" o "en")

## Tweets en tiempo real

La aplicación obtiene un stream de tweets a partir de una palabra de búsqueda, que se procesa a medida que se recibe información

Para ejecutar:
```sh
spark-submit wc_tweets_stream.py S L
```

donde `S` es la palabra a utilizar como término de búsqueda, y `L` el idioma de los tweets ("es" o "en")

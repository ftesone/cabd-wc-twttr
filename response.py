import os
import json
import requests



def get_headers():
    with open(os.path.dirname(os.path.realpath(__file__))+'/twitter_keys.json') as jf:
        keys = json.loads(jf.read())
        return {'Authorization': 'Bearer {}'.format(keys['bearer_token'])}



def get_response(method, url, payload = None, stream = False):
    method = method.upper()
    if method == 'POST':
        response = requests.post(url, headers = get_headers(), json = payload, stream = stream)
    elif method == 'GET':
        response = requests.get(url, headers = get_headers(), json = payload, stream = stream)
    if response.status_code != 200 and response.status_code != 201:
        raise Exception('Error {} on {} {}: {} with payload {}'.format(response.status_code, method, url, response.text, payload))
    return response

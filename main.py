import argparse
import base64
import json
import sys
import time
from datetime import datetime

import more_itertools
import requests
import yaml

FETCH_SIZE = 500
# TODO Fetch with argparse
last_record_id = int(sys.argv[1]) if len(sys.argv) == 2 else None
total = 0
offset = 0
fast_mode = True

parser = argparse.ArgumentParser(description='')
parser.add_argument('source_base_url', help='The base url of the source.')
parser.add_argument('source_app_id', metavar='FROM', nargs=1, help='App ID to export.')
parser.add_argument('destination_base_url', help='The base url of the destination.')
parser.add_argument('destination_app_id', metavar='TO', nargs=1, help='App ID to import.')
args = parser.parse_args()

# TODO Fetch there data from settings file.
source_base_url = args.source_base_url
source_app_id = args.source_app_id
destination_base_url = args.destination_base_url
destination_app_id = args.destination_app_id

# TODO Ask username and password if there is no .copy_record.auth file, and save it.
f = open('./.copy_record.auth', 'r')
data = yaml.load(f)

start = time.time()
previous_time = start
while True:

    # TODO Fetch the field code in order to handle the US and CN kintone app.
    payload = {
        'app': source_app_id,
        'query': '%s order by レコード番号 asc limit %d' % (
            '' if last_record_id is None else ('レコード番号 > %d' % last_record_id), FETCH_SIZE)
    }
    headers = {
        'X-Cybozu-Authorization': base64.b64encode(
            (data['origin_username'] + ':' + data['origin_password']).encode('utf-8')).decode('utf-8'),
        'User-Agent': 'Fastest kintone app backup by yuichi'
    }

    r = requests.get(source_base_url + '/k/v1/records.json?', params=payload, headers=headers)
    if r.status_code != 200:
        print(r.status_code)
        print(r.text)
        exit(1)
    body = json.loads(r.text)
    if len(body['records']) == 0:
        print(payload)
    last_record_id = int(body['records'][-1]['レコード番号']['value'].replace('contacts-', ''))

    records = []
    for record in body['records']:
        del record['レコード番号']
        del record['添付ファイル']
        del record['$revision']
        records.append(record)

    if len(records) < FETCH_SIZE:
        print(len(records))
        print('end')
        break
    total += len(records)
    print('%s [elapsed_time: %f] Now fetching %dth records. Latest record id is %d' % (
        datetime.now(), time.time() - previous_time, total, last_record_id))
    previous_time = time.time()

    for rs in more_itertools.chunked(records, 100):
        payload = {
            'app': destination_app_id,
            'records': rs
        }
        headers = {
            'X-Cybozu-Authorization': base64.b64encode(
                (data['destination_username'] + ':' + data['destination_password']).encode('utf-8')).decode('utf-8'),
            'Content-Type': 'application/json',
            'User-Agent': 'Fastest kintone app backup by yuichi'
        }
        r = requests.post(destination_base_url + '/k/v1/records.json', data=json.dumps(payload),
                          headers=headers)
        if r.status_code != 200:
            print('Failed to add ')
            print(r.text)
            exit(1)

elapsed_time = time.time() - start
print('elapsed time:{0}[sec]'.format(elapsed_time))

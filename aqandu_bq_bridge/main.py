from os import getenv
import json
import requests
import re 
import datetime 
from hashlib import md5
from google.cloud import bigquery


def _hash(x):
    return md5(str(x).encode('utf-8')).hexdigest()


def main(data, context):
    resp = requests.get(getenv("AQANDU_URL"))
    
    if resp.status_code != 200:
        return
    
    bq_client = bigquery.Client()
    table = bq_client.dataset(getenv("BQ_DATASET")).table(getenv("BQ_TABLE"))
    
    data = resp.json()
    data_f = []
    for d in data:
        if d['SensorSource'] != 'AirU': continue 
        if d['Latitude'] == 0: continue 
        if not bool(re.match(r'^[0-9A-F]{12}$', d['ID'])): continue

        bq = {
            "Timestamp": str(datetime.datetime.strptime(d['time'], "%a, %d %b %Y %H:%M:%S GMT")),
            "DeviceID": d['ID'],
            "Latitude": d['Latitude'],
            "Longitude": d['Longitude'],
            "PM2_5": d['PM2_5'],
            "Flags": 2
        }
        data_f.append(bq)

    row_ids_unique = list(map(_hash, data_f))
    errors = bq_client.insert_rows_json(
                table=table,
                json_rows=data_f,
                row_ids=row_ids_unique
             )
    if errors:
        print(errors)


if __name__ == '__main__':
    main('data', 'context')
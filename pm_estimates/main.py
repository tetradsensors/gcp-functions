import base64
import datetime
from google.cloud import storage
import json 
import requests
from dateutil.parser import parse


# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/tombo/Tetrad/global/tetrad.json'

gs_client = storage.Client()

def run_region(data):

    time   = parse(data['time'])
    region = data['name']
    lat_lo = data['lat_lo']
    lat_hi = data['lat_hi']
    lon_lo = data['lon_lo']
    lon_hi = data['lon_hi']

    time_s = time.strftime('%Y-%m-%dT%H:%M:%SZ')
    URL = f"""https://tetrad-api-qnofmwqtgq-uc.a.run.app/api/getEstimateMap?latHi={lat_hi}&latLo={lat_lo}&lonHi={lon_hi}&lonLo={lon_lo}&latSize=100&lonSize=100&time={time_s}"""

    print(URL)
    r = requests.get(URL)
    
    assert r.status_code == 200, f"ERROR in request: {r.status_code}"
    
    year_str = time.strftime('%Y')
    month_str = time.strftime('%m %B')
    

    filename = f"{year_str}/{month_str}/{region}/{region}_{time_s}.json"
    bucket = 'tetrad_estimate_maps'

    bucket = gs_client.get_bucket(bucket)
    blob = bucket.blob(filename)
    blob.upload_from_string(r.text)


def main(event, context):
    print('event:', event)
    print('context:', context)
    data = base64.b64decode(event['data']).decode('utf-8')
    print('data:', data)
    data = json.loads(data)
    run_region(data)


import os
import json
import datetime
from google.cloud import storage, pubsub_v1

gs_client = storage.Client()
viz_params = json.loads(
    gs_client.get_bucket('tetrad_server_files'
    ).get_blob('viz_params.json'
    ).download_as_string())

def format_region(region, time_s):
    info = viz_params[region]
    lats, lons = [], []
    for p in info['Boundingbox']:
        lats.append(float(p['Latitude']))
        lons.append(float(p['Longitude']))
    lat_lo = min(lats) + 1e-6
    lat_hi = max(lats) - 1e-6
    lon_lo = min(lons) + 1e-6
    lon_hi = max(lons) - 1e-6
    return {
        'lat_lo': lat_lo,
        'lat_hi': lat_hi,
        'lon_lo': lon_lo,
        'lon_hi': lon_hi,
        'name': region,
        'time': time_s
    }

def main(data, context):
    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/tetrad-296715/topics/trigger_pm_estimates'
    # publisher.create_topic(name=topic_name)

    now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    now_s = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    for region in viz_params.keys():
        print(region)
        data = format_region(region, now_s)
        message = json.dumps(data).encode()
        print(type(message))
        future = publisher.publish(topic_name, message)
        future.result()
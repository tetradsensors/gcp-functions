'''
This Cloud function is responsible for:
- Ingesting temeletry messages from AirU devices
- forwarding messages to the correct BigQuery tables
'''
import base64
import json
# from os import getenv
from hashlib import md5
import logging
import traceback
import os
from google.cloud import firestore, storage, bigquery 
from geojson import dumps as gjdumps, Point as gjPoint 
import yaml

# All the GCP Clients are persistant across instances
gs_client = storage.Client()
bq_client = bigquery.Client()
fs_client = firestore.Client()

# Download the project environment variables
env = yaml.load(
    gs_client.get_bucket(os.getenv('gs_bucket')).get_blob(os.getenv('config')).download_as_string(),
    Loader=yaml.SafeLoader)

# Information for all the regions
area_params = json.loads(
    gs_client.get_bucket(env['storage']['server_bucket']['name']
    ).get_blob(env['storage']['server_bucket']['files']['area_params']
    ).download_as_string())

env['storage']['server_bucket']['files']['area_params']
# List of all the devices that have responded. Reloaded from BigQuery after new deployment
logged_devices = set()

# Error values for relevant BigQuery fields (makes life easier below)
METRIC_ERROR_MAP = {k: v for k, v in env['airu']['errors'].items()}
airu_schema = env['airu']['fields']
bq_schema = env['bigquery']['tbl_telemetry']['schema']

# Mapping of common fields between AirU entries and BQ entries 
AIRU_BQ_MAP = {
    airu_schema['ts']:     bq_schema['ts'],
    airu_schema['id']:     bq_schema['id'],
    airu_schema['ele']:    bq_schema['ele'],
    airu_schema['pm1']:    bq_schema['pm1'],
    airu_schema['pm2_5']:  bq_schema['pm2_5'],
    airu_schema['pm10']:   bq_schema['pm10'],
    airu_schema['temp']:   bq_schema['temp'],
    airu_schema['hum']:    bq_schema['hum'],
    airu_schema['red']:    bq_schema['red'],
    airu_schema['nox']:    bq_schema['nox'],
    airu_schema['htr']:    bq_schema['htr'],
    airu_schema['rssi']:   bq_schema['rssi'],
    airu_schema['flags']:  bq_schema['flags']
}


def _hash(x):
    """
    Hash a tuple. Different from default hash() because it is consistent
    across instances, whereas hash() gets randomly seeded at the start
    of an instance. Used for BigQuery insert row_ids
    """
    return md5(str(x).encode('utf-8')).hexdigest()


def getLoggedDevices():
    """
    All Tetrad devices that have responded. After deployment the global
    variable 'logged_devices' is populated from the BigQuery table
    meta.devices. 
    """
    global logged_devices
    if len(logged_devices) == 0:
        query = f"""
        SELECT
            DeviceID
        FROM
            `{env['bigquery']['tbl_devices']['name']}`
        WHERE
            Source = "Tetrad"
        """
        job = bq_client.query(query)
        results = job.result()
        logged_devices = logged_devices.union(set([dict(r)['DeviceID'] for r in results]))


def addNewDevicesToBigQuery(new_devices:set):
    """
    Add new devices we've collected to Bigquery
    """
    if new_devices:
        rows = [{
                    env['bigquery']['tbl_devices']['schema']['id']: dev, 
                    env['bigquery']['tbl_devices']['schema']['source']: "Tetrad"
                } 
            for dev in new_devices
        ]
        row_ids = [r['DeviceID'] for r in rows]
        target_table = bq_client.dataset(env['bigquery']['tbl_devices']['ds_name']
            ).table(env['bigquery']['tbl_devices']['tbl_name'])
        errors = bq_client.insert_rows_json(
            table=target_table,
            json_rows=rows,
            row_ids=row_ids,
        )
        if errors:
            print(errors)
            return False
        else:
            return True


def addNewDevices(devices_this_call:set):
    """
    Add new devices to our global and BigQuery
    """
    global logged_devices

    # Update our list of (global) logged_devices if necessary
    getLoggedDevices()

    # Figure out which devices from this invocation have never been seen by our DB
    new_devices = devices_this_call - logged_devices

    # Add new devices to our meta.devices table
    if addNewDevicesToBigQuery(new_devices):
        
        # Add new devices to (global) logged_devices
        logged_devices = logged_devices.union(new_devices)


# http://www.eecs.umich.edu/courses/eecs380/HANDOUTS/PROJ2/InsidePoly.html
def inPolygon(p, poly):
    """
    NOTE Polygon can't cross Anti-Meridian
    @param p: Point as (Lat, Lon)
    @param poly: list of (Lat,Lon) points. Neighboring polygon vertices indicate lines
    @return: True if in poly, False otherwise
    """
    c = False
    pp = list(poly)
    N = len(poly)
    for i in range(N):
        j = (i - 1) % N
        if ((((pp[i][0] <= p[0]) and (p[0] < pp[j][0])) or
             ((pp[j][0] <= p[0]) and (p[0] < pp[i][0]))) and
            (p[1] < (pp[j][1] - pp[i][1]) * (p[0] - pp[i][0]) / (pp[j][0] - pp[i][0]) + pp[i][1])):
            c = not c
    return c


def getRegionFromPoint(p):
    """
    Search the area_params file to find the region that contains this point
    """
    
    # If Lat,Lon both equal 0.0 it didn't report GPS
    if not any(p):
        return env['bigquery']['tbl_telemetry']['labels']['badgps']

    for name, info in area_params.items():

        # Convert lat/lon bounds to a polygon 
        poly = [(p['Latitude'], p['Longitude']) for p in info['Boundingbox']]

        # If our point is in the polygon, return the "Label"
        if inPolygon(p, poly):
            return name
    
    # Region not found, give it the global label
    return env['bigquery']['tbl_telemetry']['labels']['global']


def pmIsBad(pm):
    """
    Function to determine if pm data is bad. 
    """
    return pm >= env['pm_theshold']


def main(event, context):
    try:
        entry_info = _insert_into_bigquery(event, context)
        _handle_success(entry_info)
    except Exception as e:
        _handle_error(event, e)
    
    


def _insert_into_bigquery(event, context):
    """
    Incoming MQTT packet is in 'event'. Parse the packet,
    clean it up, run the supplementary functions, then
    stream the data into BigQuery.
    """
    data = base64.b64decode(event['data']).decode('utf-8')
    
    deviceId = event['attributes']['deviceId'][1:].upper()

    row = json.loads(data)

    row[airu_schema['id']] = deviceId

    # Uploads from SD card send a timestamp, normal messages may not
    if airu_schema['ts'] not in row:
        row[airu_schema['ts']] = context.timestamp

    # Replace error codes with None - blank in BigQuery
    for k in row:
        if k in METRIC_ERROR_MAP:
            if row[k] == METRIC_ERROR_MAP[k]:
                row[k] = None 
                if k == airu_schema['nox']:
                    row[airu_schema['htr']] = None

    # Use GPS to get the correct table
    region_name = getRegionFromPoint((row[airu_schema['lat']], row[airu_schema['lon']]))
    row[bq_schema['label']] = region_name
    
    # Update GPS coordinates (so we aren't storing erroneous 0.0's in database)
    if not any((row[airu_schema['lat']], row[airu_schema['lon']])):
        row[bq_schema['gps']] = None
    else:
        # Lon then lat for geojson Point
        geo = gjPoint((row[airu_schema['lon']], row[airu_schema['lat']]))
        row[bq_schema['gps']] = gjdumps(geo)
    
    # Remove Lat/Lon
    row.pop(airu_schema['lat'], None)
    row.pop(airu_schema['lon'], None)
    

    # Filter PM:
    #   If pmIsBad() then move PM2.5 data into PM2_5_Raw, clear PMs, and set flag
    try:
        if pmIsBad(row[airu_schema['pm2_5']]):
            row[bq_schema['pmraw']] = row[airu_schema['pm2_5']]
            row[airu_schema['pm1']]   = None
            row[airu_schema['pm2_5']] = None
            row[airu_schema['pm10']]  = None
            row[airu_schema['flags']] |= 2
    except TypeError:
        pass

    # Convert AirU fields to BQ fields
    for airu_key, bq_key in AIRU_BQ_MAP.items():
        row[bq_key] = row.pop(airu_key)

    #
    # Add BigQuery Fields
    #
    row[bq_schema['pmsmodel']] = 'PMS3003'
    row[bq_schema['source']] = 'Tetrad'

    # Unique row ID constructed from only timestamp and device id
    # NOTE: Cannot use default hash() function because it is not
    #       consistent across instances, whereas md5 hash is. 
    row_ids = _hash(
        (
            row[bq_schema['ts']], 
            row[bq_schema['id']]
        )
    )

    # Add the entry to the appropriate BigQuery Table
    table = bq_client.dataset(env['bigquery']['tbl_telemetry']['ds_name']).table(env['bigquery']['tbl_telemetry']['tbl_name'])
    errors = bq_client.insert_rows_json(
        table,
        json_rows=[row],
        row_ids=[row_ids])
    if errors != []:
        print(row)
        raise BigQueryError(errors)


    # Add device to meta.devices
    addNewDevices(set([row[bq_schema['id']]]))

    return dict(
        device_id=deviceId,
        payload_bytes=len(data)
    )


def _handle_success(entry_info):
    r = dict(
        severity="DEBUG",
        **entry_info
    )
    print(json.dumps(r))


def _handle_error(event, exception):
    if 'deviceId' in event['attributes']:
        message = 'Error streaming from device \'%s\'. Cause: %s. Exception: %s.' % (event['attributes']['deviceId'], traceback.format_exc(), exception)
    else:
        message = 'Error in event: %s' % event
    print(traceback.format_exc())
    logging.error(message)


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened''' 

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
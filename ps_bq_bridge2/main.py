'''
This Cloud function is responsible for:
- Ingesting temeletry messages from AirU devices
- forwarding messages to the correct BigQuery tables
'''
import base64
import json
from os import getenv
import logging
import traceback
from datetime import datetime
from google.cloud import firestore, storage, bigquery 
from google.api_core import retry
import geojson
import pytz

bq_client = bigquery.Client()

METRIC_ERROR_MAP = {
    getenv("FIELD_ELE"):    0,
    getenv("FIELD_PM1"):    -1,
    getenv("FIELD_PM2"):    -1,
    getenv("FIELD_PM10"):   -1,
    getenv("FIELD_TEMP"):   -1000,
    getenv("FIELD_HUM"):    -1000,
    getenv("FIELD_RED"):    10000,
    getenv("FIELD_NOX"):    10000,
}

# Keep these global so they are persistant across invocations
gs_client = storage.Client()
bucket = gs_client.get_bucket(getenv("GS_BUCKET"))
blob = bucket.get_blob(getenv("GS_MODEL_BOXES"))
model_data = json.loads(blob.download_as_string())

logged_devices = set()

fs_client = firestore.Client()

def getModelBoxes():
    return model_data

 
def getLoggedDevices():
    global logged_devices
    if not logged_devices:
        query = f"""
        SELECT
            DeviceID
        FROM
            `meta.devices`
        WHERE
            Source = "Tetrad"
        """
        job = bq_client.query(query)
        results = job.result()
        logged_devices = logged_devices.union(set([dict(r)['DeviceID'] for r in results]))


def addNewDevicesToBigQuery(new_devices:set):
    if new_devices:
        rows = [{'DeviceID': dev, 'Source': "Tetrad"} for dev in new_devices]
        row_ids = [r['DeviceID'] for r in rows]
        target_table = bq_client.dataset('meta').table('devices')
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
def inPoly(p, poly):
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


def pointToTableName(p):
    if not sum(p):
        return getenv('BQ_TABLE_BADGPS')
    boxes = getModelBoxes()
    for box in boxes:
        poly = [ 
            (box['lat_hi'], box['lon_hi']), 
            (box['lat_lo'], box['lon_hi']), 
            (box['lat_lo'], box['lon_lo']), 
            (box['lat_hi'], box['lon_lo']) 
        ]
        if inPoly(p, poly):
            # print(f"Adding point {p} for bounding box {poly} to table {box['table']}")
            return box['table']
    # print(f"Adding point {p} to table {getenv('BQ_TABLE_GLOBAL')}")
    return getenv('BQ_TABLE_GLOBAL')


# def addToFirestore(mac, table):
#     fs_col = fs_client.collection('devices')
#     doc_ref = fs_col.document(mac)
#     if doc_ref.get().exists:
#         doc_ref.update({
#             getenv('FS_FIELD_LAST_BQ_TABLE'): table
#         })


def main(event, context):
    if 'data' in event:
        try:
            _insert_into_bigquery(event, context)
        except Exception:
            _handle_error(event)


def _insert_into_bigquery(event, context):
    data = base64.b64decode(event['data']).decode('utf-8')
    
    deviceId = event['attributes']['deviceId'][1:].upper()

    row = json.loads(data)
     
    row[getenv("FIELD_ID")] = deviceId

    # Uploads from SD card send a timestamp, normal messages may not
    if getenv("FIELD_TS") not in row:
        row[getenv("FIELD_TS")] = context.timestamp

    # Replace error codes with None - blank in BigQuery
    for k in row:
        if k in METRIC_ERROR_MAP:
            if row[k] == METRIC_ERROR_MAP[k]:
                row[k] = None 
                if k == getenv("FIELD_NOX"):
                    row[getenv("FIELD_HTR")] = None

    # Use GPS to get the correct table         
    table_name = pointToTableName((row[getenv("FIELD_LAT")], row[getenv("FIELD_LON")]))

    # Update GPS coordinates (so we aren't storing erroneous 0.0's in database)
    if not sum([row[getenv("FIELD_LAT")], row[getenv("FIELD_LON")]]):
        row[getenv("FIELD_GPS")] = None
    else:
        geo = geojson.Point((row[getenv('FIELD_LON')], row[getenv('FIELD_LAT')]))
        row[getenv("FIELD_GPS")] = geojson.dumps(geo)
    
    # Remove Lat/Lon
    row.pop(getenv('FIELD_LAT'), None)
    row.pop(getenv('FIELD_LON'), None)

    row[getenv('FIELD_LABEL')] = table_name
    row[getenv('FIELD_SRC')] = 'Tetrad'

    # Filter PM: Values above PM_BAD_THRESH are NULL, 
    #   and store raw PM val in FIELD_PM2_5_Raw for debug
    if row[getenv('FIELD_PM2_5')] >= int(getenv('PM_BAD_THRESH')):
        row[getenv('FIELD_PM2_5_Raw')] = row[getenv('FIELD_PM2_5')]
        row[getenv('FIELD_PM1')]    = None
        row[getenv('FIELD_PM2_5')]  = None
        row[getenv('FIELD_PM10')]   = None

    # Add the entry to the appropriate BigQuery Table
    table = bq_client.dataset(getenv('BQ_DATASET_TELEMETRY')).table(getenv('BQ_TABLE'))
    errors = bq_client.insert_rows_json(table,
                                 json_rows=[row],)
                                #  retry=retry.Retry(deadline=30))
    if errors != []:
        raise BigQueryError(errors)

    # (If no insert errors): 
    # Update FireStore entry for MAC address
    # addToFirestore(deviceId, table_name)

    # Add device to meta.devices
    addNewDevices(set([row[getenv("FIELD_ID")]]))




def _handle_success(deviceID):
    message = 'Device \'%s\' streamed into BigQuery' % deviceID
    # logging.info(message)


def _handle_error(event):
    if 'deviceId' in event['attributes']:
        message = 'Error streaming from device \'%s\'. Cause: %s' % (event['attributes']['deviceId'], traceback.format_exc())
    else:
        message = 'Error in event: %s' % event
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
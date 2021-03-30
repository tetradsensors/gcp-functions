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

bq = bigquery.Client()

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

def getModelBoxes():
    gs_client = storage.Client()
    bucket = gs_client.get_bucket(getenv("GS_BUCKET"))
    blob = bucket.get_blob(getenv("GS_MODEL_BOXES"))
    model_data = json.loads(blob.download_as_string())
    return model_data

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
            logging.info(f"Adding point {p} for bounding box {poly} to table {box['table']}")
            return box['table']
    logging.info(f"Adding point {p} to table {getenv('BQ_TABLE_GLOBAL')}")
    return getenv('BQ_TABLE_GLOBAL')


def addToFirestore(mac, table):
    fs_client = firestore.Client()
    doc_ref = fs_client.collection(getenv("FS_COL")).document(mac)
    if doc_ref.get().exists:
        doc_ref.update({
            getenv('FS_FIELD_LAST_BQ_TABLE'): table
        })


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

    print('row:', row)

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

    # Add the entry to the appropriate BigQuery Table
    table = bq.dataset(getenv('BQ_DATASET_TELEMETRY')).table(getenv('BQ_TABLE'))
    errors = bq.insert_rows_json(table,
                                 json_rows=[row],)
                                #  retry=retry.Retry(deadline=30))
    if errors != []:
        raise BigQueryError(errors)

    # (If no insert errors) Update FireStore entry for MAC address
    # addToFirestore(deviceId, table_name)


def _handle_success(deviceID):
    message = 'Device \'%s\' streamed into BigQuery' % deviceID
    logging.info(message)


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
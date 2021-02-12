from os import getenv
import logging
import datetime
import requests
import json
from google.cloud import firestore, storage
from pprint import pprint


#########################
# Following this post:
# https://cloud.google.com/blog/products/application-development/how-to-schedule-a-recurring-python-script-on-gcp
#########################


LAT_SIZE = int(getenv("LAT_SIZE"))
LON_SIZE = int(getenv("LON_SIZE"))
MAX_AGE  = int(getenv("FS_MAX_DOC_AGE_DAYS"))

URL_TEMPLATE = f"""{getenv("URL_BASE")}?src=%s&lat_size={LAT_SIZE}&lon_size={LON_SIZE}&date=%s"""
FS_CLIENT = firestore.Client()
FS_COL = FS_CLIENT.collection(getenv("FS_COLLECTION"))


def _add_tags(model_data, region, date_obj):
    model_data['region']   = region['name']
    model_data['table']    = region['table']
    model_data['lat_lo']   = region['lat_lo']
    model_data['lat_hi']   = region['lat_hi']
    model_data['lon_lo']   = region['lon_lo']
    model_data['lon_hi']   = region['lon_hi']
    model_data['lat_size'] = LAT_SIZE
    model_data['lon_size'] = LON_SIZE
    model_data['date']     = date_obj
    return model_data 


def _reformat_2dlist(model_data):
    for k,v in model_data.items():
        try:
            if isinstance(v[0], list):  # we found a list of lists
                
                # List of lists is now dict of lists with row indices as keys
                #   Also, keys are converted to strings to comply with Firestore (keys must be strings)
                model_data[k] = dict(zip(map(str, range(len(v))), v))

        except TypeError:   # value wasn't supscriptable (not list of lists), just keep going
            continue
    return model_data 


def getModelBoxes():
    gs_client = storage.Client()
    bucket = gs_client.get_bucket(getenv("GS_BUCKET"))
    blob = bucket.get_blob(getenv("GS_MODEL_BOXES"))
    model_data = json.loads(blob.download_as_string())
    return model_data


def processRegion(region):
    print(f"Procesing region: {region['qsrc']}")
    date_obj = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    date_str = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    URL = URL_TEMPLATE % (
        region['qsrc'],
        date_str
    )

    logging.warning(URL)
    
    resp = requests.get(URL)
    if resp.status_code == 200:
        final = dict()
        data = dict(resp.json())
        data = _reformat_2dlist(data)
        final['data'] = data
        final = _add_tags(final, region, date_obj)
        ret = FS_COL.document(f'{region["qsrc"]}_{date_str}').set(final)
        return ret 

    else:
        logging.warning("No data. resp: " + str(resp.status_code) + str(resp.text))
        return None 


def removeOldDocuments():
    print('removing old documents...')
    age = MAX_AGE
    date_threshold = datetime.datetime.utcnow() - datetime.timedelta(days=age)
    logging.warning('date threshold: ' + str(date_threshold))
    docs = FS_COL.where('date', '<=', date_threshold).stream()
    for doc in docs:
        print(f"Removing: {doc.id}")
        FS_COL.document(doc.id).delete()



def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    removeOldDocuments()

    logging.warning("entered function")
    model_data = getModelBoxes()
    logging.warning(model_data)
    for region in model_data:
        logging.warning(region['name'])
        processRegion(region)

    



# if __name__ == '__main__':
#     main('data', 'context')

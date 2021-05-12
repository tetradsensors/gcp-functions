# ########################################################################
from envyaml import EnvYAML
import os
env = EnvYAML('.env.yaml')
for k, v in dict(env).items():
    os.environ[k] = v
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../../global/tetrad.json'
# ########################################################################
from json import loads
from scipy.io import loadmat
from os import getenv
from io import BytesIO
import datetime
import utils
from google.cloud import storage, bigquery, firestore


def getRegionInfo():
    '''
    Get region_bounds.json from GS
    '''
    bucket = GS_CLIENT.get_bucket(getenv("GS_BUCKET"))
    blob = bucket.get_blob(getenv("GS_REGION_INFO_FILE"))
    region_info = loads(blob.download_as_string())
    region_info = {k: v for k,v in region_info.items() if v['enabled']}
    return region_info


def getElevFile(filename):
    '''
    get elev mat file from GS
    '''
    bucket = GS_CLIENT.get_bucket(getenv("GS_BUCKET"))
    blob = bucket.get_blob(filename)
    elevs_mat = loadmat(BytesIO(blob.download_as_bytes()))
    return elevs_mat


# Globals keep things fast and persistant across instances
FS_CLIENT = firestore.Client()
GS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()
REGION_INFO = getRegionInfo()
# for key in REGION_INFO.keys():
#     REGION_INFO[key]['elev_mat'] = getElevFile(REGION_INFO[key]['elev_filename'])


def queryData(regionDict):
    latlo = regionDict['lat_lo']
    lathi = regionDict['lat_hi']
    lonlo = regionDict['lon_lo']
    lonhi = regionDict['lon_hi']
    query = f"""
        SELECT 
            Timestamp, 
            DeviceID, 
            ST_Y(GPS) AS Latitude, 
            ST_X(GPS) AS Longitude, 
            PM2_5
        FROM 
            `{getenv("BQ_DATASET")}.{getenv("BQ_TABLE")}` 
        WHERE
            Timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
            AND 
            ST_WITHIN(GPS, ST_GEOGFROMTEXT('POLYGON(({lonlo} {latlo}, {lonlo} {lathi}, {lonhi} {lathi}, {lonhi} {latlo}, {lonlo} {latlo}))'))
        ORDER BY
            Timestamp ASC
    """
    print(query)
    query_job = BQ_CLIENT.query(query=query)
    rows = query_job.result()
    rows = [dict(r) for r in rows]
    return rows


def processRegion(regionDict):

    # Query data from BigQuery
    data = queryData(regionDict)
    print(f'{len(data)} rows')

    # Clean the data
    data = utils.tuneAllFields(data, ["PM2_5"], removeNulls=True)



def removeOldDocuments():
    print('removing old documents...')
    max_age = getenv("FS_MAX_DOC_AGE_DAYS")
    date_threshold = datetime.datetime.utcnow() - datetime.timedelta(days=max_age)
    col = FS_CLIENT.collection(getenv("FS_COLLECTION"))
    docs = col.where('date', '<=', date_threshold).stream()
    for doc in docs:
        print(f"Removing: {doc.id}")
        col.document(doc.id).delete()


def main(data, context):
    
    # For each region: 
    #   1. Collect and clean data in time frame /region
    #   2. Compute model
    #   3. Store in Firestore
    for k, v in REGION_INFO.items():
        print(f'processing {k}')
        processRegion(v)
        break

    # Remove old documents
    # removeOldDocuments()
    


if __name__ == '__main__':
    main('data', 'context')
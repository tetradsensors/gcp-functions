# from envyaml import EnvYAML
# import os
# env = EnvYAML('.env.yaml')
# for k, v in dict(env).items():
#     os.environ[k] = v
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../../global/tetrad.json'

from json import loads
from scipy.io import loadmat
from os import getenv
from io import BytesIO
import datetime
import utils
import gaussian_model_utils
import pytz
import numpy as np
from google.cloud import storage, bigquery, firestore
#######################################################################


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
LAT_SIZE = int(getenv("LAT_SIZE"))
LON_SIZE = int(getenv("LON_SIZE"))
FS_CLIENT = firestore.Client()
GS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()
REGION_INFO = getRegionInfo()
for key in REGION_INFO.keys():
    print(f'Downloading elevation matrix for {key}')
    REGION_INFO[key]['elev_mat'] = getElevFile(REGION_INFO[key]['elev_filename'])


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

    query_job = BQ_CLIENT.query(query=query)
    rows = query_job.result()
    rows = [dict(r) for r in rows]
    regionDict['data'] = rows
    return regionDict


def getEstimateMap(regionDict):

    query_datetime = datetime .datetime.utcnow()
    query_datetime = query_datetime.replace(tzinfo=pytz.utc)

    sensor_data = regionDict['data']

    ##################################################################
    # STEP 1: Load up length scales from file
    ##################################################################

    length_scales = utils.loadLengthScales()
    length_scales = utils.getScalesInTimeRange(length_scales, query_datetime, query_datetime)
    latlon_length_scale = length_scales[0]['latlon']
    elevation_length_scale = length_scales[0]['elevation']
    time_length_scale = length_scales[0]['time']

    ##################################################################
    # STEP 3: Convert lat/lon to UTM coordinates
    ##################################################################
    
    sensor_data = utils.convertLatLonToUTM(sensor_data)

    ##################################################################
    # STEP 4: Add elevation values to the data if missing
    ##################################################################
    
    # We need the entire elevation matrix in STEP 6, so load it 
    #   here even if we don't use it in the proceeding loop.
    elevationInterpolator = utils.setupElevationInterpolator(regionDict['elev_mat'])

    # Loop through every row and add elevation from the elevation
    #   interpolator if the row is missing elevation data
    for datum in sensor_data:
        datum['Elevation'] = elevationInterpolator([datum['Longitude']],[datum['Latitude']])[0]

    ##################################################################
    # STEP 5: Create Model
    ##################################################################
    
    model, time_offset = gaussian_model_utils.createModel(
        sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

    ##################################################################
    # STEP 6: Build the grid of query locations
    ##################################################################

    lon_vector, lat_vector = utils.interpolateQueryLocations(
        lat_lo=regionDict['lat_lo'], 
        lat_hi=regionDict['lat_hi'], 
        lon_lo=regionDict['lon_lo'], 
        lon_hi=regionDict['lon_hi'], 
        lat_size=LAT_SIZE, 
        lon_size=LON_SIZE
    )

    elevations = elevationInterpolator(lon_vector, lat_vector)
    locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)

    locations_lat = locations_lat.flatten()
    locations_lon = locations_lon.flatten()
    elevations = elevations.flatten()

    yPred, yVar = gaussian_model_utils.estimateUsingModel(
        model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

    elevations = (elevations.reshape((LAT_SIZE, LON_SIZE))).tolist()
    yPred = yPred.reshape((LAT_SIZE, LON_SIZE))
    yVar = yVar.reshape((LAT_SIZE, LON_SIZE))
    estimates = yPred.tolist()
    variances = yVar.tolist()

    response = {
        "Elevations": elevations, 
        "PM2.5": estimates, 
        "PM2.5 variance": variances, 
        "Latitudes": lat_vector.tolist(), 
        "Longitudes": lon_vector.tolist()
    }

    return response


def add_tags(model_data, region, date_obj):
    model_data['region_name'] = region['region_name']
    model_data['shortname']   = region['shortname']
    model_data['lat_lo']      = region['lat_lo']
    model_data['lat_hi']      = region['lat_hi']
    model_data['lon_lo']      = region['lon_lo']
    model_data['lon_hi']      = region['lon_hi']
    model_data['lat_size']    = LAT_SIZE
    model_data['lon_size']    = LON_SIZE
    model_data['date']        = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    return model_data 


def reformat_2dlist(model_data):
    for k,v in model_data.items():
        try:
            if isinstance(v[0], list):  # we found a list of lists
                
                # List of lists is now dict of lists with row indices as keys
                #   Also, keys are converted to strings to comply with Firestore (keys must be strings)
                model_data[k] = dict(zip(map(str, range(len(v))), v))

        except TypeError:   # value wasn't subscriptable (not list of lists), just keep going
            continue

    return model_data 


def processRegion(regionDict):

    regionDict = regionDict.copy()
    # Query data from BigQuery
    regionDict = queryData(regionDict)
    print('data len:', len(regionDict['data']))

    # Clean the data
    regionDict['data'] = utils.tuneAllFields(regionDict['data'], ["PM2_5"], removeNulls=True)

    # Compute the model
    response = getEstimateMap(regionDict)

    # Add to Firestore
    response = reformat_2dlist(response)
    final = {}
    final['data'] = response
    final = add_tags(final, regionDict, datetime.datetime.utcnow())
    collection = FS_CLIENT.collection(getenv("FS_COLLECTION"))
    collection.document(f'{final["shortname"]}_{final["date"]}').set(final)


def removeOldDocuments():
    print('removing old documents...')
    max_age = int(getenv("FS_MAX_DOC_AGE_DAYS"))
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
        try:
            processRegion(v)
        except Exception as e:
            print(str(e))

    # Remove old documents
    removeOldDocuments()
    


if __name__ == '__main__':
    final = main('data', 'context')
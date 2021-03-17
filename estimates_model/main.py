# ##################################################################
# # from envyaml import EnvYAML
# # import os 
# # env = EnvYAML('.env.yaml')
# # for k, v in dict(env).items():
# #     os.environ[k] = v
# # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../local/tetrad.json'
# ##################################################################

from os import getenv
import logging
import datetime
import requests
import json
from google.cloud import firestore, storage, bigquery
from pprint import pprint
import utils
import numpy as np
from api_consts import *
# import gaussian_model_utils

# #########################
# # Following this post:
# # https://cloud.google.com/blog/products/application-development/how-to-schedule-a-recurring-python-script-on-gcp
# #########################

# class ArgumentError(Exception):
#     status_code = 400

#     def __init__(self, message, status_code=None, payload=None):
#         Exception.__init__(self)
#         self.message = message
#         if status_code is not None:
#             self.status_code = status_code
#         else:
#             self.status_code = 400
        
#         self.payload = payload 

#     def to_dict(self):
#         rv = dict(self.payload or ())
#         rv['message'] = self.message 
#         return rv 


# class NoDataError(Exception):
#     status_code = 400

#     def __init__(self, message, status_code=None, payload=None):
#         Exception.__init__(self)
#         self.message = message
#         if status_code is not None:
#             self.status_code = status_code 
#         else:
#             self.status_code = 400
#         self.payload = payload 

#     def to_dict(self):
#         rv = dict(self.payload or ())
#         rv['message'] = self.message 
#         return rv 

# LAT_SIZE = int(getenv("LAT_SIZE"))
# LON_SIZE = int(getenv("LON_SIZE"))
# MAX_AGE  = int(getenv("FS_MAX_DOC_AGE_DAYS"))

# URL_TEMPLATE = f"""{getenv("URL_BASE")}?src=%s&lat_size={LAT_SIZE}&lon_size={LON_SIZE}&date=%s"""
# FS_CLIENT = firestore.Client()
# FS_COL = FS_CLIENT.collection(getenv("FS_COLLECTION"))


# def _add_tags(model_data, region, date_obj):
#     model_data['region']   = region['name']
#     model_data['table']    = region['table']
#     model_data['lat_lo']   = region['lat_lo']
#     model_data['lat_hi']   = region['lat_hi']
#     model_data['lon_lo']   = region['lon_lo']
#     model_data['lon_hi']   = region['lon_hi']
#     model_data['lat_size'] = LAT_SIZE
#     model_data['lon_size'] = LON_SIZE
#     model_data['date']     = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
#     return model_data 


# def _reformat_2dlist(model_data):
#     for k,v in model_data.items():
#         try:
#             if isinstance(v[0], list):  # we found a list of lists
                
#                 # List of lists is now dict of lists with row indices as keys
#                 #   Also, keys are converted to strings to comply with Firestore (keys must be strings)
#                 model_data[k] = dict(zip(map(str, range(len(v))), v))

#         except TypeError:   # value wasn't supscriptable (not list of lists), just keep going
#             continue
#     return model_data 


# def getModelBoxes():
#     gs_client = storage.Client()
#     bucket = gs_client.get_bucket(getenv("GS_BUCKET"))
#     blob = bucket.get_blob(getenv("GS_MODEL_BOXES"))
#     model_data = json.loads(blob.download_as_string())
#     return model_data


# def processRegion(region):
#     print(f"Procesing region: {region['qsrc']}")
#     date_obj = datetime.datetime.utcnow() - datetime.timedelta(minutes=15)
#     date_str = date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    
#     try:
#         response = getEstimateMap(
#             src=region['qsrc'],
#             lat_size=LAT_SIZE,
#             lon_size=LON_SIZE,
#             date='2021-02-20T00:00:00Z'
#         )

#         final = dict()
#         data = _reformat_2dlist(response)
#         final['data'] = data
#         final = _add_tags(final, region, date_obj)
#         # final = json.dumps(final)

#         ret = FS_COL.document(f'{region["qsrc"]}_{date_str}').set(final)


#     except Exception as e:
#         print('ERROR')
#         print(str(e))

    
#     # resp = requests.get(URL)
#     # if resp.status_code == 200:
#     #     final = dict()
#     #     data = dict(resp.json())
#     #     data = _reformat_2dlist(data)
#     #     final['data'] = data
#     #     final = _add_tags(final, region, date_obj)
#     #     ret = FS_COL.document(f'{region["qsrc"]}_{date_str}').set(final)
#     #     return ret 

#     # else:
#     #     logging.warning("No data. resp: " + str(resp.status_code) + str(resp.text))
#     #     return None 


def removeOldDocuments():
    print('removing old documents...')
    age = MAX_AGE
    date_threshold = datetime.datetime.utcnow() - datetime.timedelta(days=age)
    docs = FS_COL.where('date', '<=', date_threshold).stream()
    for doc in docs:
        print(f"Removing: {doc.id}")
        FS_COL.document(doc.id).delete()


# def _requestData(srcs, fields, start, end, bbox=None, removeNulls=False):
#     """
#     Function to query a field (like Temperature, Humidity, PM, etc.) 
#     or list of fields, in date range [start, end], inside a bounding
#     box. The bounding box is a tuple of (North,South,East,West) 
#     coordinates.
#     Can include an ID or a list of IDs
#     """

#     # if id_ls:
#     #     idstr = utils.idsToWHEREClause(id_ls, FIELD_MAP['DEVICEID'])
#     # else:
#     #     idstr = "True"

#     query_fields = utils.queryBuildFields(fields)

#     if bbox:
#         query_latlon = f"""
#             {FIELD_MAP["LATITUDE"]}  <= {bbox[0]}
#                 AND
#             {FIELD_MAP["LATITUDE"]}  >= {bbox[1]}
#                 AND
#             {FIELD_MAP["LONGITUDE"]} <= {bbox[2]}
#                 AND
#             {FIELD_MAP["LONGITUDE"]} >= {bbox[3]}
#         """
#     else:
#         query_latlon = "True"

#     Q_TBL = f"""
#         SELECT 
#             {query_fields}
#         FROM 
#             `{PROJECT_ID}.{BQ_DATASET_TELEMETRY}.%s` 
#         WHERE 
#             {FIELD_MAP["TIMESTAMP"]} >= "{start}"
#                 AND
#             {FIELD_MAP["TIMESTAMP"]} <= "{end}"
#                 AND
#             {query_latlon}   
#     """

#     tbl_union = utils.queryBuildSources(srcs, Q_TBL)

#     # Build the query
#     q = f"""
#         SELECT
#             {query_fields}
#         FROM 
#             ({tbl_union})
#         ORDER BY
#             {FIELD_MAP["TIMESTAMP"]};        
#     """

#     # Run the query and collect the result
#     bq_client = bigquery.Client()
#     query_job = bq_client.query(q)
#     rows = query_job.result()
    
#     # break on empty iterator
#     if rows.total_rows == 0:
#         raise NoDataError("No data returned.", status_code=222)
        
#     # Convert Response object (generator) to list-of-dicts
#     data = [dict(r) for r in rows]

#     # Clean data and apply correction factors
#     data = utils.tuneAllFields(data, fields, removeNulls=removeNulls)

#     # Apply correction factors to data
#     return data


# def _requestDataInRadius(srcs, fields, start, end, radius, center, removeNulls=False):
#     """
#     Function to query a field (like Temperature, Humidity, PM, etc.) 
#     or list of fields, in date range [start, end], inside a given
#     radius. The radius is in kilometers and center is the (Lat,Lon) 
#     center of the circle.
#     Can include an ID or a list of IDs.
#     """
#     bbox = utils.convertRadiusToBBox(radius, center)
#     data = _requestData(srcs, fields, start, end, bbox=bbox, removeNulls=removeNulls)
#     data = utils.bboxDataToRadiusData(data, radius, center)

    
#     if len(data) == 0:
#         raise NoDataError("No data returned.", status_code=222)

#     return data

# def getEstimateMap(src, lat_size, lon_size, date):
#     """
#     src
#     # lat_hi
#     # lat_lo
#     # lon_hi
#     # lon_lo
#     lat_size
#     lon_size
#     date
#     """
    
#     # TOM: Removed everything related to "UTM" source. 
#     # # this species grid positions should be interpolated in UTM coordinates
#     # if "UTM" in request.args:
#     #     UTM = True
#     # else:
#     #     UTM = False

#     # Get the arguments from the query string
#     # if not UTM:

#     # args = [
#     #     'src',
#     #     'lat_size',
#     #     'lon_size',
#     #     'date'
#     # ]

#     # req_args = [
#     #     'src', 
#     #     'lat_size',
#     #     'lon_size',
#     #     'date'
#     # ]

#     try:
#     #     utils.verifyArgs(request.args, req_args, args)
#         src = utils.argParseSources(src, single_source=True)
#         query_datetime = utils.argParseDatetime(date)
#     except ArgumentError:
#         raise
#     try:
#         lat_size = int(lat_size)
#         lon_size = int(lon_size)
#     except ValueError:
#         raise ArgumentError('lat, lon, sizes must be ints (not UTM) case', 400)

#     ##################################################################
#     # STEP 0: Load up the bounding box from file and check 
#     #         that request is within it
#     ##################################################################

#     region = utils.getModelRegion(src)
#     if not region:
#         raise ArgumentError('src bad', 400)
    
#     lat_lo = region['lat_lo']
#     lat_hi = region['lat_hi']
#     lon_lo = region['lon_lo']
#     lon_hi = region['lon_hi']



#     # TOM: replaced bounding box with "model_boxes.json" stored on Google Cloud Storage
#     # bounding_box_vertices = utils.loadBoundingBox()
#     # print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

#     # TOM: replaced query bounding box with query source and we now use predefined
#     #       bounding boxes for each city. 
#     # if not (
#     #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_lo) and
#     #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_hi) and
#     #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_hi) and
#     #     utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_lo)):
#     #     raise ArgumentError('One of the query locations is outside of the bounding box for the database', 400)

#     ##################################################################
#     # STEP 1: Load up length scales from file
#     ##################################################################

#     length_scales = utils.loadLengthScales()
#     length_scales = utils.getScalesInTimeRange(length_scales, query_datetime, query_datetime)
#     if len(length_scales) < 1:
#         msg = (
#             f"Incorrect number of length scales({len(length_scales)}) "
#             f"found in between {query_datetime}-1day and {query_datetime}+1day"
#         )
#         raise ArgumentError(msg, 400)

#     latlon_length_scale = length_scales[0]['latlon']
#     elevation_length_scale = length_scales[0]['elevation']
#     time_length_scale = length_scales[0]['time']


#     ##################################################################
#     # STEP 2: Query relevant data
#     ##################################################################

#     # Compute a circle center at the query volume.  Radius is related to lenth scale + the size of the box.
#     lat = (lat_lo + lat_hi) / 2.0
#     lon = (lon_lo + lon_hi) / 2.0

#     UTM_N_hi, UTM_E_hi, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
#     UTM_N_lo, UTM_E_lo, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
    
#     # compute the length of the diagonal of the lat-lon box.  This units here are **meters**
#     lat_diff = UTM_N_hi - UTM_N_lo
#     lon_diff = UTM_E_hi - UTM_E_lo

#     radius = TIME_KERNEL_FACTOR_PADDING * latlon_length_scale + np.sqrt(lat_diff**2 + lon_diff**2) / 2.0

#     if not ((zone_num_lo == zone_num_hi) and (zone_let_lo == zone_let_hi)):
#         raise ArgumentError('Requested region spans UTM zones', 400)
    
#     # Convert dates to strings
#     start = query_datetime - (TIME_KERNEL_FACTOR_PADDING * datetime.timedelta(hours=time_length_scale))
#     end = query_datetime + (TIME_KERNEL_FACTOR_PADDING * datetime.timedelta(hours=time_length_scale))
#     start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
#     end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

#     sensor_data = _requestDataInRadius(
#         srcs=[src], 
#         fields=[getenv("Q_PM2"), getenv("Q_ELE")], 
#         start=start_str, 
#         end=end_str, 
#         radius=radius, 
#         center=(lat, lon),
#         removeNulls=[getenv("Q_PM2")]
#     )

#     ##################################################################
#     # STEP 3: Convert lat/lon to UTM coordinates
#     ##################################################################
    
#     try:
#         sensor_data = utils.convertLatLonToUTM(sensor_data)
#     except ValueError as err:
#         return f'Error converting lat/lon to UTM: {str(err)}', 400

#     ##################################################################
#     # STEP 4: Add elevation values to the data if missing
#     ##################################################################
    
#     # We need the entire elevation matrix in STEP 6, so load it 
#     #   here even if we don't use it in the proceeding loop.
#     elevationInterpolator = utils.setupElevationInterpolatorForSource(src)

#     # Loop through every row and add elevation from the elevation
#     #   interpolator if the row is missing elevation data
#     for datum in sensor_data:
#         if ('Elevation' not in datum) or (datum['Elevation'] is None):
#             datum['Elevation'] = elevationInterpolator([datum['Longitude']],[datum['Latitude']])[0]

#     ##################################################################
#     # STEP 5: Create Model
#     ##################################################################
    
#     model, time_offset = gaussian_model_utils.createModel(
#         sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

#     ##################################################################
#     # STEP 6: Build the grid of query locations
#     ##################################################################
    
#     # if not UTM:
#     lon_vector, lat_vector = utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size)
#     # else:
#     #     return ArgumentError('UTM not yet supported', 400)

#     elevations = elevationInterpolator(lon_vector, lat_vector)
#     locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)

#     locations_lat = locations_lat.flatten()
#     locations_lon = locations_lon.flatten()
#     elevations = elevations.flatten()

#     yPred, yVar = gaussian_model_utils.estimateUsingModel(
#         model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

#     elevations = (elevations.reshape((lat_size, lon_size))).tolist()
#     yPred = yPred.reshape((lat_size, lon_size))
#     yVar = yVar.reshape((lat_size, lon_size))
#     estimates = yPred.tolist()
#     variances = yVar.tolist()

#     response = {
#                     "Elevations": elevations, 
#                     "PM2.5": estimates, 
#                     "PM2.5 variance": variances, 
#                     "Latitudes": lat_vector.tolist(), 
#                     "Longitudes": lon_vector.tolist()
#                 }  
#     return response


def main(data, context):
#     """Triggered from a message on a Cloud Pub/Sub topic.
#     Args:
#         data (dict): Event payload.
#         context (google.cloud.functions.Context): Metadata for the event.
#     """

    removeOldDocuments()
    # print("hello world!")
#     # model_data = getModelBoxes()
#     # for region in model_data:
#     #     processRegion(region)


# # if __name__ == '__main__':
# #     main('data', 'context')

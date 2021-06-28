from datetime import timedelta
import datetime
import os
import json
# from tetrad import app, bq_client, bigquery, utils, cache, jsonutils, log_client, gaussian_model_utils
import utils, jsonutils, gaussian_model_utils
from google.cloud import bigquery, firestore
# from tetrad import _area_models
# from dotenv import load_dotenv

import numpy as np
# Find timezone based on longitude and latitude
from timezonefinder import TimezoneFinder

# import psutil
# p = psutil.Process()

LAT_SIZE = 100
LON_SIZE = LAT_SIZE

# Load in .env and set the table name
# load_dotenv()  # Required for compatibility with GCP, can't use pipenv there

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/tombo/Tetrad/global/tetrad.json'
os.environ['TELEMETRY_TABLE_ID'] = 'telemetry.telemetry'
bq_client = bigquery.Client()
fs_client = firestore.Client()
with open('area_params.json') as json_file:
        json_temp = json.load(json_file)
_area_models = jsonutils.buildAreaModelsFromJson(json_temp)

# log_client.get_default_handler()
# log_client.setup_logging()

# This is now done in the submit_sensor_query in order to support multiple regions
#AIRU_TABLE_ID = os.getenv("AIRU_TABLE_ID")
#PURPLEAIR_TABLE_ID = os.getenv("PURPLEAIR_TABLE_ID")
#DAQ_TABLE_ID = os.getenv("DAQ_TABLE_ID")
# SOURCE_TABLE_MAP = {
#     "AirU": AIRU_TABLE_ID,
#     "PurpleAir": PURPLEAIR_TABLE_ID,
#     "DAQ": DAQ_TABLE_ID,
# }
# this will now we done at query time
#VALID_SENSOR_SOURCES = ["AirU", "PurpleAir", "DAQ", "all"]
TIME_KERNEL_FACTOR_PADDING = 3.0
SPACE_KERNEL_FACTOR_PADDING = 2.
MIN_ACCEPTABLE_ESTIMATE = -5.0

# the size of time sequence chunks that are used to break the eatimation/data into pieces to speed up computation
# in units of time-scale parameter
# This is a tradeoff between looping through the data multiple times and having to do the fft inversion (n^2) of large time matrices
# If the bin size is 10 mins, and the and the time scale is 20 mins, then a value of 30 would give 30*20/10, which is a matrix size of 60.  Which is not that big.  
TIME_SEQUENCE_SIZE = 20.

# constants for outier, bad sensor removal
MAX_ALLOWED_PM2_5 = 1000.0
# constant to be used with MAD estimates
DEFAULT_OUTLIER_LEVEL = 5.0
# level below which outliers won't be removed 
MIN_OUTLIER_LEVEL = 10.0


def estimateMedianDeviation(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi, area_model):
    with open('db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)

    area_id_strings=area_model['idstring']
    query_list = []
    #loop over all of the tables associated with this area model
    for area_id_string in area_id_strings:
        time_string = db_table_headings[area_id_string]['time']
        pm2_5_string = db_table_headings[area_id_string]['pm2_5']
        lon_string = db_table_headings[area_id_string]['longitude']
        lat_string = db_table_headings[area_id_string]['latitude']
        id_string = db_table_headings[area_id_string]['id']
        table_string = os.getenv(area_id_string)

        column_string = " ".join([id_string, "AS id,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon"])

        if 'sensormodel' in db_table_headings[area_id_string]:
            sensormodel_string = db_table_headings[area_id_string]['sensormodel']
            column_string += ", " + sensormodel_string + " AS sensormodel"

        if 'sensortype' in db_table_headings[area_id_string]:
            sensortype_string = db_table_headings[area_id_string]['sensortype']
            column_string += ", " + sensortype_string + " AS sensortype"

        query_list.append(f"""(SELECT pm2_5, id FROM (SELECT {column_string} FROM `{table_string}` WHERE (({time_string} > @start_date) AND ({time_string} < @end_date))) WHERE ((lat <= @lat_hi) AND (lat >= @lat_lo) AND (lon <= @lon_hi) AND (lon >= @lon_lo)) AND (pm2_5 < {MAX_ALLOWED_PM2_5}))""")

    query = "(" + " UNION ALL ".join(query_list) + ")"

    full_query = f"WITH all_data as {query} SELECT * FROM (SELECT PERCENTILE_DISC(pm2_5, 0.5) OVER() AS median FROM all_data LIMIT 1) JOIN (SELECT COUNT(DISTINCT id) as num_sensors FROM all_data) ON TRUE"

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            bigquery.ScalarQueryParameter("lat_lo", "NUMERIC", lat_lo),
            bigquery.ScalarQueryParameter("lat_hi", "NUMERIC", lat_hi),
            bigquery.ScalarQueryParameter("lon_lo", "NUMERIC", lon_lo),
            bigquery.ScalarQueryParameter("lon_hi", "NUMERIC", lon_hi),
        ]
    )

    query_job = bq_client.query(full_query, job_config=job_config)
    if query_job.error_result:
        print(query_job.error_result)
        return "Invalid API call - check documentation.", 400

    median_data = query_job.result()
    for row in median_data:
        median = row.median
        count = row.num_sensors

    full_query = f"WITH all_data as {query} SELECT PERCENTILE_DISC(ABS(pm2_5 - {median}), 0.5) OVER() AS median FROM all_data LIMIT 1"
    query_job = bq_client.query(full_query, job_config=job_config)
    if query_job.error_result:
        print(query_job.error_result)
        return "Invalid API call - check documentation.", 400
    MAD_data = query_job.result()
    for row in MAD_data:
        MAD = row.median

    return median, MAD, count


def filterUpperLowerBounds(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, filter_level = DEFAULT_OUTLIER_LEVEL):
        median, MAD, count = estimateMedianDeviation(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi, area_model)
        lo = max(median - filter_level*MAD, 0.0)
        hi = min(max(median + filter_level*MAD, MIN_OUTLIER_LEVEL), MAX_ALLOWED_PM2_5)
        return lo, hi


def getEstimateMap(areaModel, time, latSize, lonSize):

    # this species grid positions should be interpolated in UTM coordinates
    # right now (Nov 2020) this is not supported.
    # might be used later in order to build grids of data in UTM coordinates -- this would depend on what the display/visualization code needs
    # after investigation, most vis toolkits support lat-lon grids of data. 
    
    # if "UTM" in request.args:
    #     UTM = True
    # else:
    #     UTM = False
    UTM = False

    # Get the arguments from the query string
    if not UTM:
        # try:
        #     lat_hi = float(request.args.get('latHi'))
        #     lat_lo = float(request.args.get('latLo'))
        #     lon_hi = float(request.args.get('lonHi'))
        #     lon_lo = float(request.args.get('lonLo'))
        # except ValueError:
        #     return 'lat, lon, lat_res, be floats in the lat-lon (not UTM) case', 400
        try:
            lat_size = int(latSize)
            lon_size = int(lonSize)
        except ValueError:
            return 'lat, lon, sizes must be ints (not UTM) case', 400

        # lat_res = (lat_hi-lat_lo)/float(lat_size)
        # lon_res = (lon_hi-lon_lo)/float(lon_size)

    query_date = time
    # if query_date == None:
    #     query_startdate = request.args.get('startTime')
    #     query_enddate = request.args.get('endTime')
    #     if (query_startdate == None) or (query_enddate == None):
    #         return 'requires valid date or start/end', 400
    #     datesequence=True
    #     try:
    #         query_rate = float(request.args.get('timeInterval', 0.25))
    #     except ValueError:
    #         return 'timeInterval must be floats.', 400
    # else:
    #     datesequence=False
    datesequence = False

    # if "areaModel" in request.args:
    #     area_string = request.args.get('areaModel')
    # else:
    #     area_string = None

    # area_string = areaModel

    # area_model = jsonutils.getAreaModelByLocation(_area_models, lat=lat_hi, lon=lon_lo, string = area_string)
    area_model = areaModel
    # if area_model == None:
    #     msg = f"The query location, lat={lat_hi}, lon={lon_lo}, and/or area string {area_string} does not have a corresponding area model"
    #     return msg, 400

    area_model_bounds = jsonutils.getAreaModelBounds(area_model)
    lat_lo = area_model_bounds['lat_lo']
    lat_hi = area_model_bounds['lat_hi']
    lon_lo = area_model_bounds['lon_lo']
    lon_hi = area_model_bounds['lon_hi']
    lat_res = (lat_hi - lat_lo) / float(lat_size)
    lon_res = (lon_hi - lon_lo) / float(lon_size)

    print(f"Query parameters: lat_lo={lat_lo} lat_hi={lat_hi}  lon_lo={lon_lo} lon_hi={lon_hi} lat_res={lat_res} lon_res={lon_res}")
    
    # build the grid of query locations
    if not UTM:
        lon_vector, lat_vector = utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_res, lon_res)
    else:
        return 'UTM not yet supported', 400

    area_model['elevationinterpolator'] = jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])
    elevations = area_model['elevationinterpolator'](lon_vector, lat_vector)
    locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)
    query_lats = locations_lat.flatten()
    query_lons= locations_lon.flatten()
    query_elevations = elevations.flatten()
    query_locations = np.column_stack((query_lats, query_lons))

    # deal with single or time sequences.
    if not datesequence:
        query_datetime = jsonutils.parseDateString(query_date, area_model['timezone'])
        if query_datetime == None:
            msg = f"The query {query_date} is not a recognized date/time format; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
            return msg, 400
        query_dates = np.array([query_datetime])
    else:
        # query_start_datetime = jsonutils.parseDateString(query_startdate, area_model['timezone'])
        # query_end_datetime = jsonutils.parseDateString(query_enddate, area_model['timezone'])
        # if query_start_datetime == None or query_end_datetime == None:
        #     msg = f"The query ({query_startdate}, {query_enddate}) is not a recognized date/time format; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
        #     return msg, 400
        # query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)     
        return "error", 400

    yPred, yVar, status = computeEstimatesForLocations(query_dates, query_locations, query_elevations, area_model)

    num_times = len(query_dates)

    elevations = (elevations).tolist()
    yPred = yPred.reshape((lat_size, lon_size, num_times))
    yVar = yVar.reshape((lat_size, lon_size, num_times))
    # estimates = yPred.tolist()
    # variances = yVar.tolist()
    return_object = {
        "Area model": area_model["note"],
        "Elevations":elevations,
        "Latitudes":lat_vector.tolist(), 
        "Longitudes":lon_vector.tolist()
    }

    estimates = []
    for i in range(num_times):
        estimates.append(
            {
                'PM2_5': (yPred[:,:,i]).tolist(), 
                'variance': (yVar[:,:,i]).tolist(), 
                'datetime': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 
                'Status': status[i]
            }
        )

    return_object['estimates'] = estimates
    return return_object


# submit a query for a range of values
# Ross Nov 2020
# this has been consolidate and generalized so that multiple api calls can use the same query code
def submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_id_strings, min_value, max_value):

    with open('db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)

    query_list = []
    #loop over all of the tables associated with this area model
    for area_id_string in area_id_strings:
        time_string = db_table_headings[area_id_string]['time']
        pm2_5_string = db_table_headings[area_id_string]['pm2_5']
        lon_string = db_table_headings[area_id_string]['longitude']
        lat_string = db_table_headings[area_id_string]['latitude']
        id_string = db_table_headings[area_id_string]['id']
        table_string = os.getenv(area_id_string)

        column_string = " ".join([id_string, "AS id,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon"])

        if 'sensormodel' in db_table_headings[area_id_string]:
            sensormodel_string = db_table_headings[area_id_string]['sensormodel']
            column_string += ", " + sensormodel_string + " AS sensormodel"

        if 'sensortype' in db_table_headings[area_id_string]:
            sensortype_string = db_table_headings[area_id_string]['sensortype']
            column_string += ", " + sensortype_string + " AS sensortype"

        query_list.append(f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}` WHERE (({time_string} > @start_date) AND ({time_string} < @end_date))) WHERE ((lat <= @lat_hi) AND (lat >= @lat_lo) AND (lon <= @lon_hi) AND (lon >= @lon_lo)) AND (pm2_5 < {MAX_ALLOWED_PM2_5}))""")

    query = " UNION ALL ".join(query_list) + " ORDER BY time ASC "

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            bigquery.ScalarQueryParameter("lat_lo", "NUMERIC", lat_lo),
            bigquery.ScalarQueryParameter("lat_hi", "NUMERIC", lat_hi),
            bigquery.ScalarQueryParameter("lon_lo", "NUMERIC", lon_lo),
            bigquery.ScalarQueryParameter("lon_hi", "NUMERIC", lon_hi),
        ]
    )

    query_job = bq_client.query(query, job_config=job_config)

    if query_job.error_result:
        print(query_job.error_result)
        return "Invalid API call - check documentation.", 400
    # Waits for query to finish
    sensor_data = query_job.result()

    return sensor_data


# radius should be in *meters*!!!
# this has been modified so that it now takes an array of lats/lons
# the radius parameter is not implemented in a precise manner -- rather it is converted to a lat-lon bounding box and all within that box are returned
# there could be an additional culling of sensors outside the radius done here after the query - if the radius parameter needs to be precise. 
def request_model_data_local(lats, lons, radius, start_date, end_date, area_model, outlier_filtering = True):
    model_data = []
    # get the latest sensor data from each sensor
    # Modified by Ross for
    ## using a bounding box in lat-lon
    if isinstance(lats, (float)):
            if isinstance(lons, (float)):
                    lat_lo, lat_hi, lon_lo, lon_hi = utils.latlonBoundingBox(lats, lons, radius)
            else:
                    return "lats,lons data structure misalignment in request sensor data", 400
    elif (isinstance(lats, (np.ndarray)) and isinstance(lons, (np.ndarray))):
        if not lats.shape == lons.shape:
            return "lats,lons data data size error", 400
        else:
            num_points = lats.shape[0]
            lat_lo, lat_hi, lon_lo, lon_hi = utils.latlonBoundingBox(lats[0], lons[0], radius)
            for i in range(1, num_points):
                lat_lo, lat_hi, lon_lo, lon_hi = utils.boundingBoxUnion((utils.latlonBoundingBox(lats[i], lons[i], radius)), (lat_lo, lat_hi, lon_lo, lon_hi))
    else:
        return "lats,lons data structure misalignment in request sensor data", 400

    if outlier_filtering:
        min_value, max_value = filterUpperLowerBounds(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model)
    else:
        min_value = 0.0
        max_value = MAX_ALLOWED_PM2_5
    rows = submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model["idstring"], min_value, max_value)

    for row in rows:
        new_row = {
            "ID": str(row.id),
            "Latitude": row.lat,
            "Longitude": row.lon,
            "time": row.time,
            "PM2_5": row.pm2_5,
            }
        if 'sensormodel' in row:
            new_row["SensorModel"] = row.sensormodel
        else:
            new_row["SensorModel"] = "Default"

        if 'sensorsource' in row:
            new_row["SensorModel"] = row.sensorsource
        else:
            new_row["SensorSource"] = "Default"

        model_data.append(new_row)

    return model_data


# this is a generic helper function that sets everything up and runs the model
def computeEstimatesForLocations(query_dates, query_locations, query_elevations, area_model, outlier_filtering = True):
    num_locations = query_locations.shape[0]
    query_lats = query_locations[:,0]
    query_lons = query_locations[:,1]
    query_start_datetime = query_dates[0]
    query_end_datetime = query_dates[-1]

    # step 2, load up length scales from file

    latlon_length_scale, time_length_scale, elevation_length_scale = jsonutils.getLengthScalesForTime(area_model['lengthscales'], query_start_datetime)
    if latlon_length_scale == None:
            print("No length scale found between dates {query_start_datetime} and {query_end_datetime}")
            return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Length scale parameter error" for i in range(query_dates.shape[0])]

    # step 3, query relevent data

    # radius is in meters, as is the length scale and UTM.    
    radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale

    sensor_data = request_model_data_local(
            query_lats,
            query_lons,
            radius,
            query_start_datetime - timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
            query_end_datetime + timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
            area_model, outlier_filtering)
    unique_sensors = {datum['ID'] for datum in sensor_data}
    print(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        print(str(err))
        return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Failure to convert lat/lon" for i in range(query_dates.shape[0])]

    unique_sensors = {datum['ID'] for datum in sensor_data}

    # Step 4, parse sensor type from the version
    #    sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '0000', 'Default':'Default'}
    # DAQ does not need a correction factor
    #    for datum in sensor_data:
    #        datum['type'] =  sensor_source_to_type[datum['SensorSource']]

    if len(sensor_data) > 0:
        # print(f'Fields: {sensor_data[0].keys()}')
        pass
    else:
        print(f'Got zero sensor data')
        return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Zero sensor data" for i in range(query_dates.shape[0])]

    # step 4.5, Data Screening
    #    print('Screening data')
    sensor_data = utils.removeInvalidSensors(sensor_data)

    # step 5, apply correction factors to the data
    for datum in sensor_data:
        datum['PM2_5'] = jsonutils.applyCorrectionFactor(area_model['correctionfactors'], datum['time'], datum['PM2_5'], datum['SensorModel'])

    # step 6, add elevation values to the data
    # NOTICE - the elevation object takes locations in the form "lon-lat"
    # this seems redundant since elevations are passed in...
    for datum in sensor_data:
        if 'Altitude' not in datum:
            datum['Altitude'] = area_model['elevationinterpolator']([datum['Longitude']],[datum['Latitude']])[0]
    
    time_padding = timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale)
    time_sequence_length = timedelta(hours = TIME_SEQUENCE_SIZE*time_length_scale)
    sensor_sequence, query_sequence = utils.chunkTimeQueryData(query_dates, time_sequence_length, time_padding)

    yPred = np.empty((num_locations, 0))
    yVar = np.empty((num_locations, 0))
    status = []
    if len(sensor_data) == 0:
        status = "0 sensors/measurements"
        return 
    for i in range(len(query_sequence)):
    # step 7, Create Model
        model, time_offset, model_status = gaussian_model_utils.createModel(
            sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale, sensor_sequence[i][0], sensor_sequence[i][1], save_matrices=True)
        # check to see if there is a valid model
        if (model == None):
            yPred_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), 0.0)
            yVar_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), np.nan)
            status_estimate_tmp = [model_status for i in range(len(query_sequence[i]))]
        else:
            yPred_tmp, yVar_tmp, status_estimate_tmp = gaussian_model_utils.estimateUsingModel(
                model, query_lats, query_lons, query_elevations, query_sequence[i], time_offset, save_matrices=True)
        # put the estimates together into one matrix
        yPred = np.concatenate((yPred, yPred_tmp), axis=1)
        yVar = np.concatenate((yVar, yVar_tmp), axis=1)
        status = status + status_estimate_tmp

    if np.min(yPred) < MIN_ACCEPTABLE_ESTIMATE:
        print("got estimate below level " + str(MIN_ACCEPTABLE_ESTIMATE))
        
    # Here we clamp values to ensure that small negative values to do not appear
    yPred = np.clip(yPred, a_min = 0., a_max = None)

    return yPred, yVar, status


def removeOldDocuments():
    max_age_days = 15
    date_threshold = datetime.datetime.utcnow() - datetime.timedelta(days=max_age_days)
    col = fs_client.collection('estimateMaps')
    docs = col.where('date', '<=', date_threshold).stream()
    for doc in docs:
        col.document(doc.id).delete()


def reformat_2dlist(list2d):            
    # List of lists is now dict of lists with row indices as keys
    #   Also, keys are converted to strings to comply with Firestore (keys must be strings)
    return dict(zip(map(str, range(len(list2d))), list2d))


def format_obj(areaModel, d):
    estimates = d['estimates'][0]
    data = {
        'PM2.5': reformat_2dlist(estimates['PM2_5']),
        'PM2.5 variance': reformat_2dlist(estimates['variance']),
        'Elevations': reformat_2dlist(d['Elevations']),
        'Latitudes': d['Latitudes'],
        'Longitudes': d['Longitudes']
    }
    
    final = {
        'lat_size': LAT_SIZE,
        'lon_size': LON_SIZE,
        'lat_lo': min(d['Latitudes']),
        'lat_hi': max(d['Latitudes']),
        'lon_lo': min(d['Longitudes']),
        'lon_hi': max(d['Longitudes']),
        'shortname': areaModel['shortname'],
        'region_name': areaModel['note'],
        'date': estimates['datetime'],
        'data': data
    }

    return final



def main(data, context):
    
    for area_string in ['Cleveland', 'Kansas_City', 'Chattanooga', 'Salt_Lake_City', 'Pioneer_Valley']:
        print(area_string)
        time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:00Z')

        areaModel = jsonutils.getAreaModelByLocation(_area_models, string=area_string)

        estimate_obj = getEstimateMap(areaModel, time, LAT_SIZE, LON_SIZE)

        # Add to Firestore
        obj = format_obj(areaModel, estimate_obj)
        collection = fs_client.collection('estimateMaps')
        doc_name = f'{obj["shortname"]}_{obj["date"]}'
        collection.document(doc_name).set(obj)
        print(f'saved to {doc_name}')

    removeOldDocuments()


if __name__ == '__main__':
    # main('data', 'context')
    
    areaModel = jsonutils.getAreaModelByLocation(_area_models, string='Chattanooga')

    estimate_obj = getEstimateMap(areaModel, '2021-06-19T05:00:00Z', latSize=100, lonSize=100)
    from matplotlib import pyplot as plt 
    plt.imshow(estimate_obj['estimates'][0]['PM2_5'], origin='lower')
    plt.show()
    # import json
    # with open('/Users/tombo/Downloads/gcp_estimates_chatt_2021-06-19.json', 'w') as handle:
    #     json.dump(estimate_obj, handle)
    # print('saved to', '/Users/tombo/Downloads/gcp_estimates_chatt_2021-06-19.json')
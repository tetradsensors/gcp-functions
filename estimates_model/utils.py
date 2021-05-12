# from os import getenv
from datetime import datetime, timedelta
import dateutil
from dateutil import parser as dateutil_parser
# from pytz import timezone
from utm import from_latlon
# from matplotlib.path import Path
from scipy import interpolate
from scipy.io import loadmat
from csv import reader as csv_reader
import math 
# from flask import jsonify
import numpy as np
# import re
# from google.cloud import storage
import json
from classes import ArgumentError
from api_consts import *

# DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S+0000"
# BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# def getModelBoxes():
#     gs_client = storage.Client()
#     bucket = gs_client.get_bucket(getenv("GS_BUCKET"))
#     blob = bucket.get_blob(getenv("GS_MODEL_BOXES"))
#     model_data = json.loads(blob.download_as_string())
#     return model_data
# MODEL_BOXES = getModelBoxes()


# def getModelRegion(src):
#     """
#     MODEL_BOXES is a list of dicts stored in Google Cloud Storage.
#     Each dict in the list looks like this:
#     {
#         "name": "Salt Lake City, Utah",
#         "table": "slc_ut",
#         "qsrc": "SLC",
#         "lat_hi": 40.806852,
#         "lat_lo": 40.644519,
#         "lon_hi": -111.811118,
#         "lon_lo": -111.971465
#     }
#     """
#     for r in MODEL_BOXES:
#         if r['qsrc'] == src:
#             return r
#     return None
            

def parseDatetimeString(datetime_string:str):
    """Parse date string into a datetime object"""
    
    datetime_obj = dateutil_parser.parse(datetime_string, yearfirst=True, dayfirst=False)
    
    # If user didn't specify a timezone, assume they meant UTC. Re-parse using UTC timezone.
    if datetime_obj.tzinfo is None:
        datetime_obj = datetime_obj.replace(tzinfo=dateutil.tz.UTC)
    return datetime_obj


# # def datetimeToBigQueryTimestamp(date):
# #     try:
# #         return date.strftime(BQ_DATETIME_FORMAT)
# #     except AttributeError:
# #         return None


# # # Load up elevation grid
# def setupElevationInterpolator():
#     elevInterps = {}
#     print('setupElevationInterpolator')
#     for k, v in ELEV_MAPS.items():
#         print(k)
#         data = loadmat(v)
#         elevs_grid = data['elevs']
#         lats_arr = data['lats']
#         lons_arr = data['lons']
#         print(lats_arr.shape, lons_arr.shape, elevs_grid.shape)
#         elevInterps[k] = interpolate.interp2d(lons_arr, lats_arr, elevs_grid, kind='cubic')
#     print('Finished loading')
#     return elevInterps


def setupElevationInterpolator(elev_mat):
    elevs_grid = elev_mat['elevs']
    lats_arr = elev_mat['lats']
    lons_arr = elev_mat['lons']
    return interpolate.interp2d(lons_arr, lats_arr, elevs_grid, kind='cubic')


# def loadBoundingBox():
#     with open(getenv("BOUNDING_BOX_FILENAME")) as csv_file:
#         read_csv = csv_reader(csv_file, delimiter=',')
#         rows = [row for row in read_csv][1:]
#         bounding_box_vertices = [(index, float(row[1]), float(row[2])) for row, index in zip(rows, range(len(rows)))]
#         return bounding_box_vertices


# def loadCorrectionFactors():
#     with open(getenv("CORRECTION_FACTORS_FILENAME")) as csv_file:
#         read_csv = csv_reader(csv_file, delimiter=',')
#         rows = [row for row in read_csv]
#         header = rows[0]
#         rows = rows[1:]
#         correction_factors = []
#         for row in rows:
#             rowDict = {name: elem for elem, name in zip(row, header)}
#             rowDict['start_date'] = parseDatetimeString(rowDict['start_date'])
#             rowDict['end_date']   = parseDatetimeString(rowDict['end_date'])
#             rowDict['3003_slope'] = float(rowDict['3003_slope'])
#             rowDict['3003_intercept'] = float(rowDict['3003_intercept'])
#             correction_factors.append(rowDict)
#         return correction_factors


def applyCorrectionFactor(factors, data_timestamp, data):
    for factor in factors:
        factor_start = factor['start_date']
        factor_end = factor['end_date']
        if factor_start <= data_timestamp and factor_end > data_timestamp:
            return max(0, data * factor['3003_slope'] + factor['3003_intercept'])
    print('\nNo correction factor found for ', data_timestamp)
    return data


def applyCorrectionFactorsToList(data_list, pm25_key=None):
    """Apply correction factors (in place) to PM2.5 data in data_list"""
    
    # Open the file and get correction factors
    with open(getenv("CORRECTION_FACTORS_FILENAME")) as csv_file:
        read_csv = csv_reader(csv_file, delimiter=',')
        rows = [row for row in read_csv]
        header = rows[0]
        rows = rows[1:]
        correction_factors = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDatetimeString(rowDict['start_date'])
            rowDict['end_date'] = parseDatetimeString(rowDict['end_date'])
            rowDict['3003_slope'] = float(rowDict['3003_slope'])
            rowDict['3003_intercept'] = float(rowDict['3003_intercept'])
            correction_factors.append(rowDict)
        
    # Apply the correction factors to the PM2.5 data
    for datum in data_list:
        try:
            datum[pm25_key] = applyCorrectionFactor(correction_factors, datum['Timestamp'], datum[pm25_key])
        except: # Only try once. We just assume it isn't there if the first row doesn't have it
            return data_list
        # found = False
        # for factor in correction_factors:
        #     factor_start = factor['start_date']
        #     factor_end = factor['end_date']
        #     if factor_start <= datum['Timestamp'] < factor_end:
        #         datum['PM2_5'] = datum['PM2_5'] * factor['3003_slope'] + factor['3003_intercept']
        #         found = True
        #         break
        # if not found:
        #     print('\nNo correction factor found for ', datum['Timestamp'])
    return data_list


def _tuneData(data:list, pm25_key=None, temp_key=None, hum_key=None, removeNulls=False):
    """ Clean data and apply correction factors """
    # Open the file and get correction factors
    if pm25_key:
        with open(getenv("CORRECTION_FACTORS_FILENAME")) as csv_file:
            read_csv = csv_reader(csv_file, delimiter=',')
            rows = [row for row in read_csv]
            header = rows[0]
            rows = rows[1:]
            correction_factors = []
            for row in rows:
                rowDict = {name: elem for elem, name in zip(row, header)}
                rowDict['start_date'] = parseDatetimeString(rowDict['start_date'])
                rowDict['end_date'] = parseDatetimeString(rowDict['end_date'])
                rowDict['3003_slope'] = float(rowDict['3003_slope'])
                rowDict['3003_intercept'] = float(rowDict['3003_intercept'])
                correction_factors.append(rowDict)
        
    goodPM, goodTemp, goodHum = True, True, True
    for datum in data:
        if pm25_key and goodPM:
            try:
                if (datum[pm25_key] == getenv("PM_BAD_FLAG")) or (datum[pm25_key] >= getenv("PM_BAD_THRESH")):
                    datum[pm25_key] = None
                else:
                    datum[pm25_key] = applyCorrectionFactor(correction_factors, datum['Timestamp'], datum[pm25_key])
            except:
                goodPM = False

        if temp_key and goodTemp:
            try:
                if datum[temp_key] == getenv("TEMP_BAD_FLAG"):
                    datum[temp_key] = None 
            except:
                goodTemp = False

        if hum_key and goodHum:
            try:
                if datum[hum_key] == getenv("HUM_BAD_FLAG"):
                    datum[hum_key] = None 
            except:
                goodHum = False
    
    if removeNulls:

        # If True, remove all rows with Null data
        if isinstance(removeNulls, bool):
            len_before = len(data)
            data = [datum for datum in data if all(datum.values())]
            len_after = len(data)
        
        # If it's a list, remove the rows missing data listed in removeNulls list        
        elif isinstance(removeNulls, list):
            if verifyFields(removeNulls):
                # Make sure each of the fields specified by removeNulls is in the row. 
                data = [datum for datum in data if all([datum[field] for field in removeNulls])]
            else:
                raise ArgumentError(f"(Internal error): removeNulls bad field name: {removeNulls}", 500)
        
        else:
            raise ArgumentError(f"(Internal error): removeNulls must be bool or list, but was: {type(removeNulls)}", 500)

    return data
        

def tuneAllFields(data, fields, removeNulls=False):
    return _tuneData(
            data,
            pm25_key=(FIELD_MAP["PM2_5"] if "PM2_5" in fields else None),
            temp_key=(FIELD_MAP["TEMPERATURE"] if "TEMPERATURE" in fields else None),
            hum_key=(FIELD_MAP["HUMIDITY"] if "HUMIDITY" in fields else None),
            removeNulls=removeNulls,
    )


def loadLengthScales():
    with open(getenv("LENGTH_SCALES_FILENAME")) as csv_file:
        read_csv = csv_reader(csv_file, delimiter=',')
        rows = [row for row in read_csv]
        header = rows[0]
        rows = rows[1:]
        length_scales = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDatetimeString(rowDict['start_date'])
            rowDict['end_date'] = parseDatetimeString(rowDict['end_date'])
            rowDict['latlon'] = float(rowDict['latlon'])
            rowDict['elevation'] = float(rowDict['elevation'])
            rowDict['time'] = float(rowDict['time'])
            length_scales.append(rowDict)
        return length_scales


# def isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
#     verts = [(0, 0)] * len(bounding_box_vertices)
#     for elem in bounding_box_vertices:
#         verts[elem[0]] = (elem[2], elem[1])
#     # Add first vertex to end of verts so that the path closes properly
#     verts.append(verts[0])
#     codes = [Path.MOVETO]
#     codes += [Path.LINETO] * (len(verts) - 2)
#     codes += [Path.CLOSEPOLY]
#     boundingBox = Path(verts, codes)
#     return boundingBox.contains_point((query_lon, query_lat))


# def removeInvalidSensors(sensor_data):
#     # sensor is invalid if its average reading for any day exceeds 350 ug/m3
#     epoch = datetime(1970, 1, 1)
#     epoch = timezone('US/Mountain').localize(epoch)
#     dayCounts = {}
#     dayReadings = {}

#     # Accumulate total PM and # entries for each sensor and each day
#     for datum in sensor_data:
#         pm25 = datum['PM2_5']
#         datum['daysSinceEpoch'] = (datum['Timestamp'] - epoch).days
#         key = (datum['daysSinceEpoch'], datum['DeviceID'])
#         if key in dayCounts:
#             dayCounts[key] += 1
#             dayReadings[key] += pm25
#         else:
#             dayCounts[key] = 1
#             dayReadings[key] = pm25

#     # get days that had higher than 350 avg reading
#     keysToRemove = [key for key in dayCounts if (dayReadings[key] / dayCounts[key]) > 350]
#     keysToRemoveSet = set()
#     for key in keysToRemove:
#         keysToRemoveSet.add(key)
#         keysToRemoveSet.add((key[0] + 1, key[1]))
#         keysToRemoveSet.add((key[0] - 1, key[1]))

#     print(f'Removing these days from data due to exceeding 350 ug/m3 avg: {keysToRemoveSet}')
#     sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['DeviceID']) not in keysToRemoveSet]

#     # TODO NEEDS TESTING!
#     # 5003 sensors are invalid if Raw 24-hour average PM2.5 levels are > 5 ug/m3
#     # AND the two sensors differ by more than 16%
#     # sensor5003Locations = {
#     #     datum['ID']: (datum['utm_x'], datum['utm_y']) for datum in sensor_data if datum['type'] == '5003'
#     # }
#     # sensorMatches = {}
#     # for sensor in sensor5003Locations:
#     #     for match in sensor5003Locations:
#     #         if sensor5003Locations[sensor] == sensor5003Locations[match] and sensor != match:
#     #             sensorMatches[sensor] = match
#     #             sensorMatches[match] = sensor
#     #
#     # keysToRemoveSet = set()
#     # for key in dayReadings:
#     #     sensor = key[1]
#     #     day = key[0]
#     #     if sensor in sensorMatches:
#     #         match = sensorMatches[sensor]
#     #         reading1 = dayReadings[key] / dayCounts[key]
#     #         key2 = (day, match)
#     #         if key2 in dayReadings:
#     #             reading2 = dayReadings[key2] / dayCounts[key2]
#     #             difference = abs(reading1 - reading2)
#     #             maximum = max(reading1, reading2)
#     #             if min(reading1, reading2) > 5 and difference / maximum > 0.16:
#     #                 keysToRemoveSet.add(key)
#     #                 keysToRemoveSet.add((key[0] + 1, key[1]))
#     #                 keysToRemoveSet.add((key[0] - 1, key[1]))
#     #                 keysToRemoveSet.add(key2)
#     #                 keysToRemoveSet.add((key2[0] + 1, key2[1]))
#     #                 keysToRemoveSet.add((key2[0] - 1, key2[1]))
#     #
#     # print((
#     #     "Removing these days from data due to pair of 5003 sensors with both > 5 "
#     #     f"daily reading and smaller is 16% different reading from larger : {keysToRemoveSet}"
#     # ))
#     # sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['ID']) not in keysToRemoveSet]

#     # * Otherwise just average the two readings and correct as normal.
#     return sensor_data


def getScalesInTimeRange(scales, start_time, end_time):
    relevantScales = []
    if start_time == end_time:
        start_time = start_time - timedelta(days=1)
        end_time = end_time + timedelta(days=1)
    for scale in scales:
        scale_start = scale['start_date']
        scale_end = scale['end_date']
        if start_time < scale_end and end_time >= scale_start:
            relevantScales.append(scale)
    return relevantScales


# def interpolateQueryDates(start_datetime, end_datetime, period):
#     query_dates = []
#     query_date = start_datetime
#     while query_date <= end_datetime:
#         query_dates.append(query_date)
#         query_date = query_date + timedelta(hours=period)

#     return query_dates

def interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size):
    lat_vector = np.linspace(lat_lo, lat_hi, lat_size)
    lon_vector = np.linspace(lon_lo, lon_hi, lon_size)

    return lon_vector, lat_vector


def latlonToUTM(lat, lon):
    return from_latlon(lat, lon)


# # TODO: Rename
def convertLatLonToUTM(sensor_data):
    for datum in sensor_data:
        datum['utm_x'], datum['utm_y'], datum['zone_num'], _ = latlonToUTM(datum['Latitude'], datum['Longitude'])
    return sensor_data

def convertRadiusToBBox(r, c):
    '''
    Latitude:  1 deg = 110.54 km
    Longitude: 1 deg = 111.320*cos(latitude) km
    '''
    N = c[0] + (r / 110540)
    S = c[0] - (r / 110540)
    E = c[1] + (r / (111320 * math.cos(math.radians(c[0]))))
    W = c[1] - (r / (111320 * math.cos(math.radians(c[0]))))
    return [N, S, E, W]


# https://www.movable-type.co.uk/scripts/latlong.html
def distBetweenCoords(p1, p2):
    """
    Get the Great Circle Distance between two
    GPS coordinates, in kilometers
    """
    R = 6371
    phi1 = p1[0] * (math.pi / 180)
    phi2 = p2[0] * (math.pi / 180)
    del1 = (p2[0] - p1[0]) * (math.pi / 180)
    del2 = (p2[1] - p1[1]) * (math.pi / 180)
    
    a = math.sin(del1 / 2) * math.sin(del1 / 2) +   \
        math.cos(phi1) * math.cos(phi2) *           \
        math.sin(del2 / 2) * math.sin(del2 / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = R * c 
    return d

def coordsInCircle(coords, radius, center):
    return distBetweenCoords(coords, center) <= radius


def bboxDataToRadiusData(data, radius, center):
    inRad = []
    for datum in data:
        lat = datum[FIELD_MAP["LATITUDE"]]
        lon = datum[FIELD_MAP["LONGITUDE"]]
        if coordsInCircle((lat, lon), radius, center):
            inRad.append(datum)
    return inRad


# def idsToWHEREClause(ids, id_field_name):
#     """
#     Return string that looks like:
#     (<id_field_name> = <id[0]> OR ... OR <id_field_name> = <id[n-1]>)
#     """
#     if isinstance(ids, str):
#         ids = [ids]

#     return """({})""".format(' OR '.join([f'{id_field_name} = "{ID}"' for ID in ids]))


# def verifyDateString(dateString:str) -> bool:
#     """Check if date string is valid"""
#     try:
#         return bool(dateutil_parser.parse(dateString, yearfirst=True, dayfirst=False))
#     except dateutil_parser.ParserError:
#         return False


# def verifyLatitude(lat:float) -> bool:
#     """Check if latitude is valid"""
#     return (-90 <= lat <= 90)


# def verifyLongitude(lon:float) -> bool:
#     """Check if longitude is valid"""
#     return (-180 <= lon <= 180)


# def verifyLatLon(lat:float, lon:float) -> bool:
#     """Check if lat/lon are valid"""
#     return verifyLatitude(lat) and verifyLongitude(lon)


# def verifyRadius(radius:float) -> bool:
#     """Check if valid radius for Earth in kilometers"""
#     return (0 < radius < 6371)


# def verifyDeviceString(device:str) -> bool:
#     """
#     Must be 12-character HEX string in CAPS
#     Forcing caps is delibrate so that it won't 
#     make it past this check and into a query (where it will fail)
#     """
#     return bool(re.match(r'^[0-9A-F]{12}$', device))


# def verifyDeviceList(devices:[str]) -> bool:
#     """
#     Check list of devices (12-char HEX strings)
#     Require ALL devices to be valid. This is intentional 
#     instead of filtering out bad IDs because the user
#     might not notice that some devices are incorrect.
#     """
#     return all(map(verifyDeviceString, devices))


def verifySources(srcs:list):
    return set(srcs).issubset(SRC_MAP)


def verifyFields(fields:list):
    return set(fields).issubset(FIELD_MAP)


# def verifyRequiredArgs(request_args, required_args):
#     if any(elem not in list(request_args) for elem in list(required_args)):
#         raise ArgumentError(f'Missing arg, one of: {", ".join(list(required_args))}', status_code=400)
#     return True


# def verifyPossibleArgs(request_args, possible_args):
#     if not set(request_args).issubset(set(possible_args)):
#         raise ArgumentError(f'Argument outside of possible argument list: [{", ".join(list(possible_args))}]', status_code=400)
#     return True 


# def verifyArgs(request_args, required_args, possible_args):
#     try:
#         verifyRequiredArgs(request_args, required_args)
#         verifyPossibleArgs(request_args, possible_args)
#     except ArgumentError:
#         raise
#     return True


# def argParseLat(lat):
#     try:
#         verifyLatitude(lat)
#     except:
#         raise ArgumentError("Must supply valid latitude", 400)
#     return lat


# def argParseLon(lon):
#     try:
#         verifyLongitude(lon)
#     except:
#         raise ArgumentError("Must supply valid longitude", 400)
#     return lon


def argParseSources(srcs, single_source=False):
    '''
    Parse a 'src' argument from request.args.get('src')
    into a list of sources
    '''

    if ',' in srcs:
        
        if single_source:
            raise ArgumentError(f"Argument 'src' must be one included from: {', '.join(SRC_MAP)}", 400)

        srcs = [s.upper() for s in srcs.split(',')]
    else:
        srcs = [srcs.upper()]
    
    if len(srcs) > 1 and "ALL" in srcs:
        return "Argument list cannot contain 'ALL' and other sources", 400
    if len(srcs) > 1 and "ALLGPS" in srcs:
        return "Argument list cannot contain 'ALLGPS' and other sources", 400
    
    if "ALLGPS" in srcs:
        srcs = list(ALLGPS_TBLS)

    # Check src[s] for validity
    if not verifySources(srcs):
        raise ArgumentError(f"Argument 'src' must be included from one or more of {', '.join(SRC_MAP)}", 400)
    
    if single_source:
        return srcs[0]
    else:
        return srcs


# def argParseFields(fields):
#     # Multiple fields?
#     if ',' in fields:
#         fields = [s.upper() for s in fields.split(',')]
#     else:
#         fields = [fields.upper()]

#     # Check field[s] for validity -- all fields must be in FIELD_MAP to pass
#     if not verifyFields(fields):
#         raise ArgumentError(f"Argument 'field' must be included from one or more of {', '.join(FIELD_MAP)}", status_code=400)
#     return fields


# def argParseDevices(devices_str:str):
#     if devices_str is None:
#         return devices_str 

#     if ',' in devices_str:
#         devices = [s.upper() for s in devices_str.split(',')]
#     else:
#         devices = [devices_str.upper()]
    
#     if not verifyDeviceList(devices):
#         raise ArgumentError(f"Argument 'device' must be 12-digit HEX string or list of strings", 400)
#     return devices


def argParseDatetime(datetime_str:str):
    try:
        return parseDatetimeString(datetime_str)
    except dateutil_parser.ParserError:
        raise ArgumentError(f'Invalid datetime format. Correct format is: "{DATETIME_FORMAT}". For URL encoded strings, a (+) must be replaced with (%2B). See https://www.w3schools.com/tags/ref_urlencode.ASP for all character encodings.', status_code=400)


# def argParseBBox(bbox:str):
#     if bbox is None:
#         return bbox 
#     try:
#         bb = list(map(float, bbox.split(',')))
#         if not (verifyLatLon(bb[0], bb[2]) and verifyLatLon(bb[1], bb[3])):
#             raise Exception
#         if bb[0] <= bb[2] or bb[1]<=bb[3]:
#             raise Exception
#         return bb
#     except:
#         raise ArgumentError("Argument 'box' error. 'box' must be list of latitudes and longitudes in the order of North,South,East,West.", status_code=400)


# def argParseRadius(r:float):
#     if r is None:
#         return r

#     if not verifyRadius(r):
#         raise ArgumentError("Argument 'radius' must be a float between 0 and 6371 (kilometers)", status_code=400)
#     return r


# def argParseCenter(c:str):
#     if c is None:
#         return c

#     try:
#         lat, lon = list(map(float, c.split(',')))
#         if verifyLatLon(lat, lon):
#             return (lat, lon)
#     except:
#         raise ArgumentError("Argument 'center' must be a valid pair of latitude,longitude coordinates, such as 'center=88.1,-110.2242", status_code=400)


# def argParseRadiusArgs(r:float, c:str):
#     """
#     Parse both radius and center arguments. 
#     - If neither is specified return None. 
#     - If only one is specified return error.
#     - If both are specified return the pair as a tuple
#     """
#     try:
#         x = (argParseRadius(r), argParseCenter(c))
#         if all(x): 
#             return x
#         elif not any(x): 
#             return None
#         else:
#             raise ArgumentError("Arguments 'radius' and 'center' must both be specified. Argument 'radius' must be a float between 0 and 6371 (kilometers) and argument 'center' must be a valid pair of latitude,longitude coordinates, such as 'center=88.1,-110.2242", status_code=400)
#     except ArgumentError:
#         raise


def queryBuildFields(fields):
    # Build the 'fields' portion of query
    q_fields = f"""{FIELD_MAP["DEVICEID"]}, 
                   {FIELD_MAP["TIMESTAMP"]}, 
                   {FIELD_MAP["LATITUDE"]}, 
                   {FIELD_MAP["LONGITUDE"]},
                   {','.join(FIELD_MAP[field] for field in fields)}
                """
    return q_fields


def queryBuildSources(srcs, query_template):
    """
    turns a list of bigquery table names and a query
    template into a union of the queries across the sources
    """
    if srcs[0] == "ALL":
        tbl_union = query_template % ('*')
    elif len(srcs) == 1:
        tbl_union = query_template % (SRC_MAP[srcs[0]])
    else:
        tbl_union = '(' + ' UNION ALL '.join([query_template % (SRC_MAP[s]) for s in srcs]) + ')'
    
    return tbl_union


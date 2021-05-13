# from os import getenv
from datetime import timedelta
import dateutil
from dateutil import parser as dateutil_parser
from utm import from_latlon
from scipy import interpolate
from scipy.io import loadmat
from csv import reader as csv_reader
import numpy as np
import json
from classes import ArgumentError
from api_consts import *
            

def parseDatetimeString(datetime_string:str):
    """Parse date string into a datetime object"""
    
    datetime_obj = dateutil_parser.parse(datetime_string, yearfirst=True, dayfirst=False)
    
    # If user didn't specify a timezone, assume they meant UTC. Re-parse using UTC timezone.
    if datetime_obj.tzinfo is None:
        datetime_obj = datetime_obj.replace(tzinfo=dateutil.tz.UTC)
    return datetime_obj


def setupElevationInterpolator(elev_mat):
    elevs_grid = elev_mat['elevs']
    lats_arr = elev_mat['lats']
    lons_arr = elev_mat['lons']
    return interpolate.interp2d(lons_arr, lats_arr, elevs_grid, kind='cubic')


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


def interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_size, lon_size):
    lat_vector = np.linspace(lat_lo, lat_hi, lat_size)
    lon_vector = np.linspace(lon_lo, lon_hi, lon_size)

    return lon_vector, lat_vector


def latlonToUTM(lat, lon):
    return from_latlon(lat, lon)


# # # TODO: Rename
def convertLatLonToUTM(sensor_data):
    for datum in sensor_data:
        datum['utm_x'], datum['utm_y'], datum['zone_num'], _ = latlonToUTM(datum['Latitude'], datum['Longitude'])
    return sensor_data

# def convertRadiusToBBox(r, c):
#     '''
#     Latitude:  1 deg = 110.54 km
#     Longitude: 1 deg = 111.320*cos(latitude) km
#     '''
#     N = c[0] + (r / 110540)
#     S = c[0] - (r / 110540)
#     E = c[1] + (r / (111320 * math.cos(math.radians(c[0]))))
#     W = c[1] - (r / (111320 * math.cos(math.radians(c[0]))))
#     return [N, S, E, W]


# # https://www.movable-type.co.uk/scripts/latlong.html
# def distBetweenCoords(p1, p2):
#     """
#     Get the Great Circle Distance between two
#     GPS coordinates, in kilometers
#     """
#     R = 6371
#     phi1 = p1[0] * (math.pi / 180)
#     phi2 = p2[0] * (math.pi / 180)
#     del1 = (p2[0] - p1[0]) * (math.pi / 180)
#     del2 = (p2[1] - p1[1]) * (math.pi / 180)
    
#     a = math.sin(del1 / 2) * math.sin(del1 / 2) +   \
#         math.cos(phi1) * math.cos(phi2) *           \
#         math.sin(del2 / 2) * math.sin(del2 / 2)
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
#     d = R * c 
#     return d

# def coordsInCircle(coords, radius, center):
#     return distBetweenCoords(coords, center) <= radius


# def bboxDataToRadiusData(data, radius, center):
#     inRad = []
#     for datum in data:
#         lat = datum[FIELD_MAP["LATITUDE"]]
#         lon = datum[FIELD_MAP["LONGITUDE"]]
#         if coordsInCircle((lat, lon), radius, center):
#             inRad.append(datum)
#     return inRad


# def verifySources(srcs:list):
#     return set(srcs).issubset(SRC_MAP)


def verifyFields(fields:list):
    return set(fields).issubset(FIELD_MAP)


# def argParseSources(srcs, single_source=False):
#     '''
#     Parse a 'src' argument from request.args.get('src')
#     into a list of sources
#     '''

#     if ',' in srcs:
        
#         if single_source:
#             raise ArgumentError(f"Argument 'src' must be one included from: {', '.join(SRC_MAP)}", 400)

#         srcs = [s.upper() for s in srcs.split(',')]
#     else:
#         srcs = [srcs.upper()]
    
#     if len(srcs) > 1 and "ALL" in srcs:
#         return "Argument list cannot contain 'ALL' and other sources", 400
#     if len(srcs) > 1 and "ALLGPS" in srcs:
#         return "Argument list cannot contain 'ALLGPS' and other sources", 400
    
#     if "ALLGPS" in srcs:
#         srcs = list(ALLGPS_TBLS)

#     # Check src[s] for validity
#     if not verifySources(srcs):
#         raise ArgumentError(f"Argument 'src' must be included from one or more of {', '.join(SRC_MAP)}", 400)
    
#     if single_source:
#         return srcs[0]
#     else:
#         return srcs


# def argParseDatetime(datetime_str:str):
#     try:
#         return parseDatetimeString(datetime_str)
#     except dateutil_parser.ParserError:
#         raise ArgumentError(f'Invalid datetime format. Correct format is: "{DATETIME_FORMAT}". For URL encoded strings, a (+) must be replaced with (%2B). See https://www.w3schools.com/tags/ref_urlencode.ASP for all character encodings.', status_code=400)


# def queryBuildFields(fields):
#     # Build the 'fields' portion of query
#     q_fields = f"""{FIELD_MAP["DEVICEID"]}, 
#                    {FIELD_MAP["TIMESTAMP"]}, 
#                    {FIELD_MAP["LATITUDE"]},     
#                    {FIELD_MAP["LONGITUDE"]},
#                    {','.join(FIELD_MAP[field] for field in fields)}
#                 """
#     return q_fields


# def queryBuildSources(srcs, query_template):
#     """
#     turns a list of bigquery table names and a query
#     template into a union of the queries across the sources
#     """
#     if srcs[0] == "ALL":
#         tbl_union = query_template % ('*')
#     elif len(srcs) == 1:
#         tbl_union = query_template % (SRC_MAP[srcs[0]])
#     else:
#         tbl_union = '(' + ' UNION ALL '.join([query_template % (SRC_MAP[s]) for s in srcs]) + ')'
    
#     return tbl_union


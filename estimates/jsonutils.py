from matplotlib.path import Path
import pytz
from datetime import datetime
from dateutil.parser import parse
from dateutil.utils import default_tzinfo
from dateutil import tz
import numpy as np
from scipy import interpolate
from scipy.io import loadmat

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

JAN_FIRST = datetime(2000, 1, 1, 0, 0, 0, 0, pytz.timezone('UTC'))
JAN_LAST = datetime(2100, 1, 1, 0, 0, 0, 0, pytz.timezone('UTC'))


def getAreaModelBounds(area_model):
    area_bounds = area_model['boundingbox']
    bounds = dict()
    bounds['lat_hi'] = area_bounds[0][1]
    bounds['lon_hi'] = area_bounds[1][2]
    bounds['lat_lo'] = area_bounds[2][1]
    bounds['lon_lo'] = area_bounds[0][2]
    if bounds['lat_hi'] <= bounds['lat_lo'] or bounds['lon_hi'] < bounds['lon_lo']:
        return None
    else:
        return bounds


def parseDateString(datetime_string, dflt_tz_string=None):
    if (dflt_tz_string == None):
        try:
            return parse(datetime_string)
        except:
            return None
    else:
        dflt_tz = tz.gettz(dflt_tz_string)
        try:
            return default_tzinfo(parse(datetime_string), dflt_tz)
        except:
            return None


def loadBoundingBox(bbox_info):
        rows = [row for row in bbox_info]
        bounding_box_vertices = [(index, float(row['Latitude']), float(row['Longitude'])) for row, index in zip(rows, range(len(rows)))]
        return bounding_box_vertices


def loadCorrectionFactors(cfactor_info, dflt_tz_string=None):
    cfactors = {}
    for sensor_type in cfactor_info:
        sensorDict = []
        for row in cfactor_info[sensor_type]:
    # need to deal with default values
            new_row = {}
            if (row['starttime'] != 'default'):
                new_row['starttime'] = parseDateString(row['starttime'], dflt_tz_string)
                new_row['endtime'] = parseDateString(row['endtime'], dflt_tz_string)
            new_row['slope'] = float(row['slope'])
            new_row['intercept'] = float(row['intercept'])
            new_row['note'] = row['note']
            sensorDict.append(new_row)
    # put the default at the end of the list -- the system will use whichever one hits first
        for row in cfactor_info[sensor_type]:
            if (row['starttime'] == 'default'):
                new_row['starttime'] = JAN_FIRST
                new_row['endtime'] = JAN_LAST
                new_row['slope'] = float(row['slope'])
                new_row['intercept'] = float(row['intercept'])
                new_row['note'] = row['note']
                sensorDict.append(new_row)
        cfactors[sensor_type] = sensorDict
    return cfactors


# put the default at the end of the list -- the system will use whichever one hits first
def loadLengthScales(length_info, dflt_tz_string=None):
    lengthScaleArray = []
    for row in length_info:
        new_row = {}
        if (row['starttime'] != 'default'):
            new_row['starttime'] = parseDateString(row['starttime'], dflt_tz_string)
            new_row['endtime'] = parseDateString(row['endtime'], dflt_tz_string)
            new_row['Space'] = float(row['Space'])
            new_row['Time'] = float(row['Time'])
            new_row['Elevation'] = float(row['Elevation'])
            lengthScaleArray.append(new_row)
    for row in length_info:
        if (row['starttime'] == 'default'):
            new_row['starttime'] = JAN_FIRST
            new_row['endtime'] = JAN_LAST
            new_row['Space'] = float(row['Space'])
            new_row['Time'] = float(row['Time'])
            new_row['Elevation'] = float(row['Elevation'])
            lengthScaleArray.append(new_row)
    return lengthScaleArray


def isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
    verts = [(0, 0)] * len(bounding_box_vertices)
    for elem in bounding_box_vertices:
        verts[elem[0]] = (elem[2], elem[1])
    # Add first vertex to end of verts so that the path closes properly
    verts.append(verts[0])
    codes = [Path.MOVETO]
    codes += [Path.LINETO] * (len(verts) - 2)
    codes += [Path.CLOSEPOLY]
    boundingBox = Path(verts, codes)
    return boundingBox.contains_point((query_lon, query_lat))


def getAreaModelByLocation(area_models, lat=0.0, lon=0.0, string=None):
    if string is None:
        for key in area_models:
            if (isQueryInBoundingBox(area_models[key]['boundingbox'], lat, lon)):
                print(f'Using area_model for {key}')
                return area_models[key]
    else:
        try:
            return area_models[string]
        except:
            print("Got bad request for area by string: " + str(string))

    print("Query location "+str(lat)+ "," + str(lon) + " not in any known model area")
    return None


def buildAreaModelsFromJson(json_data):
    area_models = {}
    for key in json_data:
        this_model = {}
        this_model['shortname'] = json_data[key]['shortname']
        this_model['timezone'] = json_data[key]['Timezone']
        this_model['idstring'] = json_data[key]['ID String']
        this_model['elevationfile'] = json_data[key]['Elevation File']
        this_model['note'] = json_data[key]['Note']
        # this_model['elevationinterpolator'] = buildAreaElevationInterpolator(json_data[key]['Elevation File'])
        this_model['elevationinterpolator'] = None
        this_model['boundingbox'] = loadBoundingBox(json_data[key]['Boundingbox'])
        this_model['correctionfactors'] = loadCorrectionFactors(json_data[key]['Correction Factors'],json_data[key]['Timezone'])
        this_model['lengthscales'] = loadLengthScales(json_data[key]['Length Scales'], json_data[key]['Timezone'])
        if 'Source table map' in json_data[key]:
            this_model['sourcetablemap'] = json_data[key]['Source table map']
        # else:
        #     this_model['sourcetablemap'] = None
        area_models[key] = this_model
    return area_models


def applyCorrectionFactor(factors, data_timestamp, data, sensor_type, status=False):
    for factor_type in factors:
        if sensor_type == factor_type:
            for i in range(len(factors[factor_type])):
                if factors[factor_type][i]['starttime'] <= data_timestamp and factors[factor_type][i]['endtime'] > data_timestamp:
                    if not status:
                        return np.maximum(data * factors[factor_type][i]['slope'] + factors[factor_type][i]['intercept'], 0.0)
                    else:
                        # print(f"factor type is {factor_type} and case {i}")
                        # print(factors[factor_type][i])
                        return np.maximum(data * factors[factor_type][i]['slope'] + factors[factor_type][i]['intercept'], 0.0), factors[factor_type][i]['note']
#  no correction factor will be considered identity
    if not status:
        return data
    else:
        return data, "no correction"


def getLengthScalesForTime(length_scales_array, datetime):
    for i in range(len(length_scales_array)):
        if length_scales_array[i]['starttime'] <= datetime and length_scales_array[i]['endtime'] > datetime:
            return length_scales_array[i]['Space'], length_scales_array[i]['Time'], length_scales_array[i]['Elevation']
    print("failure to find length scale in area model")
    return None, None, None


def buildAreaElevationInterpolator(filename):
    data = loadmat(filename)
    elevation_grid = data['elevs']
    gridLongs = data['lons']
    gridLats = data['lats']
    sz = ((elevation_grid.size + gridLats.size + gridLongs.size) * 8) / 1e6
    print(f'Elevation map file size: {sz} MB')
    return interpolate.interp2d(gridLongs, gridLats, elevation_grid, kind='linear', fill_value=0.0)

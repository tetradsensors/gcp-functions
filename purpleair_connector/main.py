from google.cloud import bigquery, storage
import json
import pandas as pd
import requests
import os
import datetime
import geojson
import numpy as np 
from matplotlib.path import Path
from time import sleep
import yaml


gs_client = storage.Client()
# Download the project environment variables
env = yaml.load(
    gs_client.get_bucket(os.getenv('gs_bucket')).get_blob(os.getenv('config')).download_as_string(),
    Loader=yaml.SafeLoader)


def loadBoundingBox(bbox_info):
        rows = [row for row in bbox_info]
        bounding_box_vertices = [(index, float(row['Latitude']), float(row['Longitude'])) for row, index in zip(rows, range(len(rows)))]
        return bounding_box_vertices


def buildAreaModelsFromJson(json_data):
    area_models = {}
    for key in json_data:
        this_model = {}
        this_model['name'] = json_data[key]['Name']
        this_model['timezone'] = json_data[key]['Timezone']
        this_model['idstring'] = json_data[key]['ID String']
        this_model['elevationfile'] = json_data[key]['Elevation File']
        this_model['note'] = json_data[key]['Note']
        # this_model['elevationinterpolator'] = buildAreaElevationInterpolator(json_data[key]['Elevation File'])
        this_model['elevationinterpolator'] = None
        this_model['boundingbox'] = loadBoundingBox(json_data[key]['Boundingbox'])
        # this_model['correctionfactors'] = loadCorrectionFactors(json_data[key]['Correction Factors'],json_data[key]['Timezone'])
        # this_model['lengthscales'] = loadLengthScales(json_data[key]['Length Scales'], json_data[key]['Timezone'])
        if 'Source table map' in json_data[key]:
            this_model['sourcetablemap'] = json_data[key]['Source table map']
        # else:
        #     this_model['sourcetablemap'] = None
        area_models[key] = this_model
    return area_models


# Information for all the regions
_area_params = buildAreaModelsFromJson(
    json.loads(
        gs_client.get_bucket(env['storage']['server_bucket']['name']
        ).get_blob(env['storage']['server_bucket']['files']['area_params']
        ).download_as_string()))


def getAreaModelBounds(area_model):
    area_bounds = area_model['boundingbox']
    bounds = {
        'lat_hi': area_bounds[0][1],
        'lon_hi': area_bounds[1][2],
        'lat_lo': area_bounds[2][1],
        'lon_lo': area_bounds[3][2]
    }
    
    if bounds['lat_hi'] <= bounds['lat_lo'] or bounds['lon_hi'] < bounds['lon_lo']:
        return None
    else:
        return bounds


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


def applyRegionalLabelsToDataFrame(regions_dict, df, null_value=np.nan):
    df['Label'] = null_value

    for region_name, region_info in regions_dict.items():
        bbox = getAreaModelBounds(region_info)
        
        df.loc[
            (df['Lat'] >= bbox['lat_lo']) &
            (df['Lat'] <= bbox['lat_hi']) &
            (df['Lon'] >= bbox['lon_lo']) &
            (df['Lon'] <= bbox['lon_hi']),
            'Label'
        ] = region_info['name']
        
        print(f"For region {region_name}, applied labels to {len(df[df['Label'] == region_info['name']])} rows")

    print(f"Regional labels applied to {len(df[~df['Label'].isnull()])} out of {len(df)} rows. ({int(100 * (len(df[~df['Label'].isnull()]) / len(df)))})")
    return df


def applyRegionalLabelsToDataFrameAndTrim(regions_info, df):
    df = applyRegionalLabelsToDataFrame(regions_info, df)
    return df.dropna(subset=['Label'])


def chunk_list(ls, chunk_size=10000):
    '''
    BigQuery only allows inserts <=10,000 rows
    '''
    for i in range(0, len(ls), chunk_size):
        yield ls[i: i + chunk_size]


def setPMSModels(df, col_name):
    pms_models = ['PMS1003', 'PMS3003', 'PMS5003', 'PMS7003']
    for model in pms_models:
        df.loc[df['Type'].str.contains(model), col_name] = model
    return df


def setChildFromParent(df, pairings, col_name):
    df.loc[pairings.index, col_name] = df.loc[pairings, col_name].values
    return df


def getParentChildPairing(df):
    '''
    Purple Air devices have two PM sensors inside. Data is reported for both sensors seperately,
    but one sensor is considered the "parent" and one is the "child". The child has
    lots of missing information, like DEVICE_LOCATIONTYPE, Flag, Type. So we create
    this Series to link parents and children, then later use this Series to fill in
    missing data for the children with data from their parents. 
    
    Beware: sometimes we find orphans - rows with a non-null ParentID, but no corresponding 
    row with an ID equal to the value of that ParentID. 
    '''

    # Get the rows where ParentID is not Null (ParentID values are the IDs of the parent sensors)
    pairings = df['ParentID'].loc[~df['ParentID'].isnull()].astype(int)

    # Eliminate orphans (sorry orphans)
    pairings = pairings[pairings.isin(df.index)]

    return pairings


def main(data, context):

    # with open('area_params.json') as json_file:
    #     json_temp = json.load(json_file)
    # _area_models = buildAreaModelsFromJson(json_temp)

    response = None
    try:
        response = json.loads(requests.get('https://www.purpleair.com/json?a').text)
        results = response['results']
    except Exception as e:
        print('Could not download data. Exception: ', str(e), response)
        try:
            print('trying again after 15 seconds')
            sleep(15)
            response = json.loads(requests.get('https://www.purpleair.com/json?a').text)
            results = response['results']
        except:
            print('Could not download data (take 2). Exception: ', str(e), response)
            return
            

    # Convert JSON response to a Pandas DataFrame
    df = pd.DataFrame(results)
    print(f'rows: {len(df)}')

    if df.empty:
        return

    # Trim off old data
    df['LastSeen'] = pd.to_datetime(df['LastSeen'], unit='s', utc=True)

    # Remove all datapoints older than 6 minutes. Run every 5 minutes, so give 1 minute overlap
    df = df[df['LastSeen'] > (pd.Timestamp.utcnow() - pd.Timedelta(6, unit='minutes'))]

    # Following Series operations depend on having an index
    df = df.set_index('ID')

    # Get Series with index = child ID and 'ParentID' = Parent ID
    pairing = getParentChildPairing(df)
    
    # Set DEVICE_LOCATIONTYPE child to DEVICE_LOCATIONTYPE parent
    df = setChildFromParent(df, pairing, 'DEVICE_LOCATIONTYPE')

    # Set flag of child sensor to flag of parent sensor
    df = setChildFromParent(df, pairing, 'Flag')

    # Set the 'Type' string of child to that of parent
    df = setChildFromParent(df, pairing, 'Type')

    # Use 'Flag', 'A_H', 'Hidden' to filter out bad data
    #   'Flag': Data flagged for unusually high readings
    #   'A_H': true if the sensor output has been downgraded or marked for attention due to suspected hardware issues
    #   'Hidden': Hide from public view on map: true/false
    df = df.fillna({
        'A_H': False, 
        'Flag': False, 
        'Hidden': False
        })
    
    # Convert JSON 'true'/'false' strings into bools
    df = df.replace({'true': True, 'false': False})

    # Change types
    df = df.astype({
        'A_H': bool, 
        'Flag': bool, 
        'Hidden': bool,
        'PM2_5Value': float,
        'temp_f': float,
        'LastSeen': str,
        })

    # If any of these are true, remove the row
    df['Flag'] = df['Flag'] | df['A_H'] | df['Hidden']

    # Remove rows
    df = df[df['DEVICE_LOCATIONTYPE'] == 'outside']     # Remove sensors not outside
    df = df[df['Flag'] != 1]                            # Remove sensors with Flag, A_H, or Hidden flags
    df = df.dropna(subset=['Lat', 'Lon'])               # Remove sensors with no lat/lon info

    # Apply regional labels ('slc_ut', 'chatt_tn', etc.)
    df = applyRegionalLabelsToDataFrame(_area_params, df)

    # Create the GPS column
    df['GPS'] = df.apply(lambda x: geojson.dumps(geojson.Point((x['Lon'], x['Lat']))), axis=1)

    # Move bad PM data out of cleaned column
    df['PM2_5_Raw'] = df.loc[df['PM2_5Value'] >= float(env['pm_threshold']), 'PM2_5Value']
    df['Flags'] = 0
    df.loc[df['PM2_5Value'] >= float(env['pm_threshold']), 'Flags'] |= 2
    df.loc[df['PM2_5Value'] >= float(env['pm_threshold']), 'PM2_5Value'] = np.nan

    # Convert temperature F to C
    df['temp_f'] = (df['temp_f'] - 32) * (5. / 9)

    # clean up PMS 'Type' names
    df = setPMSModels(df, col_name='PMSModel')

    # Reduce DataFrame to desired columns
    df = df.reset_index()
    cols_to_keep = ['LastSeen', 'ID', 'GPS', 'PM2_5Value', 'PM2_5_Raw', 'Flags', 'PMSModel', 'humidity', 'temp_f', 'pressure', 'Label']
    df = df.loc[:, df.columns.isin(cols_to_keep)]

    # Append 'PP' to device id's
    df['ID'] = 'PP' + df['ID'].astype(str)

    # Add 'Source' = 'PurpleAir'
    df['Source'] = 'PurpleAir'

    # Finally, convert NaN to None
    df = df.replace({np.nan: None})

    # Rename columns
    df = df.rename({
        'LastSeen':     'Timestamp',
        'ID':           'DeviceID', 
        'PM2_5Value':   'PM2_5',
        'humidity':     'Humidity', 
        'temp_f':       'Temperature',
        'pressure':     'Pressure'
    }, axis='columns')


    # Convert dataframe to list of dicts
    data = df.to_dict('records')

    # Create unique row_ids to avoid duplicates when inserting overlapping data
    row_ids = pd.util.hash_pandas_object(df[['Timestamp', 'DeviceID']]).values.astype(str)

    bq_client = bigquery.Client()
    target_table = bq_client.dataset(env['bigquery']['tbl_telemetry']['ds_name']).table(env['bigquery']['tbl_telemetry']['tbl_name'])
    
    # Maximum upload size for BigQuery API is 10,000 rows, so we have to upload the data in chunks
    for i, (data_chunk, rows_chunk) in enumerate(zip(chunk_list(data, chunk_size=10000), chunk_list(row_ids, chunk_size=10000))):
        print(f'Sending chunk {i + 1}...')
        errors = bq_client.insert_rows_json(
            table=target_table,
            json_rows=data_chunk,
            row_ids=rows_chunk
        )
        if errors:
            print(errors)
        else:
            print(f"Inserted {len(rows_chunk)} rows")


if __name__ == '__main__':
    
    # This only runs locally
    import os
    env['bigquery']['tbl_telemetry']['ds_name']  = "dev"
    env['bigquery']['tbl_telemetry']['tbl_name'] = "dev"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../../global/tetrad.json'
    main('data', 'context')
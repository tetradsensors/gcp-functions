from google.cloud.bigquery import Client
import json
import pandas as pd
import requests
from os import getenv
import datetime
import geojson
import numpy as np 
    

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


def setChildFromParent(df, child_parent_Series, col_name):
    child_parent_Series = child_parent_Series.dropna()
    df.loc[child_parent_Series.index, col_name] = df.loc[child_parent_Series, col_name].values
    return df


def main(data, context):

    try:
        response = json.loads(requests.get('https://www.purpleair.com/json?a').text)
        results = response['results']
    except Exception as e:
        print('Exception: ', str(e), response)
        return

        # Convert JSON response to a Pandas DataFrame
    df = pd.DataFrame(results)
    print(f'rows: {len(df)}')

    if df.empty:
        return

    # Trim off old data
    df['LastSeen'] = pd.to_datetime(df['LastSeen'], unit='s', utc=True)

    # Run every 5 minutes, so give 1 minute overlap
    df = df[df['LastSeen'] > (pd.Timestamp.utcnow() - pd.Timedelta(6, unit='minutes'))]

    # Following Series operations depend on having an index
    df = df.set_index('ID')

    # Get Series with index = child ID and 'ParentID' = Parent ID
    par = df['ParentID'].loc[~df['ParentID'].isnull()].astype(int)
    
    # Set DEVICE_LOCATIONTYPE child to DEVICE_LOCATIONTYPE parent
    df = setChildFromParent(df, par, 'DEVICE_LOCATIONTYPE')

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

    # Set flag of child sensor to flag of parent sensor
    df = setChildFromParent(df, par, 'Flag')

    # Set the 'Type' string of child to that of parent
    df = setChildFromParent(df, par, 'Type')

    # Remove rows
    df = df[df['DEVICE_LOCATIONTYPE'] == 'outside'] # Must be outside
    df = df[df['Flag'] != 1]    # Bad data
    df = df.dropna(subset=['Lat', 'Lon'])   # No Lat/Lon

    # Create the GPS column
    df['GPS'] = df.apply(lambda x: geojson.dumps(geojson.Point((x['Lon'], x['Lat']))), axis=1)

    # Move bad PM data out of cleaned column
    df['PM2_5_Raw'] = df.loc[df['PM2_5Value'] >= float(getenv('PM_BAD_THRESH')), 'PM2_5Value']
    df['Flags'] = 0
    df.loc[df['PM2_5Value'] >= float(getenv('PM_BAD_THRESH')), 'Flags'] |= 2
    df.loc[df['PM2_5Value'] >= float(getenv('PM_BAD_THRESH')), 'PM2_5Value'] = np.nan

    # Convert temperature F to C
    df['temp_f'] = (df['temp_f'] - 32) * (5. / 9)

    # clean up PMS 'Type' names
    df = setPMSModels(df, col_name='PMSModel')

    # Reduce DataFrame to desired columns
    df = df.reset_index()
    cols_to_keep = ['LastSeen', 'ID', 'GPS', 'PM2_5Value', 'PM2_5_Raw', 'Flags', 'PMSModel', 'humidity', 'temp_f', 'pressure']
    df = df.loc[:, df.columns.isin(cols_to_keep)]

    # Append 'PP' to device id's
    df['ID'] = 'PP' + df['ID'].astype(str)

    # Add 'Source' = 'PurpleAir'
    df['Source'] = 'PurpleAir'

    # Finally, convert NaN to None
    df = df.replace({np.nan: None})

    # Rename columns
    df = df.rename({
        'LastSeen': 'Timestamp',
        'ID': 'DeviceID', 
        'PM2_5Value': 'PM2_5',
        'humidity': 'Humidity', 
        'temp_f': 'Temperature',
        'pressure': 'Pressure'
    }, axis='columns')


    # Convert dataframe to list of dicts
    data = df.to_dict('records')

    # Create unique row_ids to avoid duplicates when inserting overlapping data
    row_ids = pd.util.hash_pandas_object(df).values.astype(str)

    print('cleaned...')

    client = Client()
    target_table = client.dataset(getenv("BQ_DATASET")).table(getenv("BQ_TABLE"))
    
    for i, (data_chunk, rows_chunk) in enumerate(zip(chunk_list(data, chunk_size=10000), chunk_list(row_ids, chunk_size=10000))):
        print(f'Sending chunk {i + 1}...')
        errors = client.insert_rows_json(
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
    os.environ['BQ_TABLE'] = "telemetry"
    os.environ['BQ_DATASET'] = "dev"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../../global/tetrad.json'
    main('data', 'context')
from google.cloud.bigquery import Client
from hashlib import md5
from os import getenv

client = Client()

# This global will remain persistant across invocations 
# if called again within 15 minutes
logged_devices = set()

def _hash(x):
    return md5(str(x).encode('utf-8')).hexdigest()


def getLoggedDevices():
    global logged_devices
    if not logged_devices:
        query = f"""
        SELECT
            DeviceID
        FROM
            `meta.devices`
        WHERE
            Source = "AQ&U"
        """
        job = client.query(query)
        results = job.result()
        logged_devices = logged_devices.union(set([dict(r)['DeviceID'] for r in results]))


def addNewDevicesToBigQuery(new_devices:set):
    if new_devices:
        rows = [{'DeviceID': dev, 'Source': "AQ&U"} for dev in new_devices]
        target_table = client.dataset('meta').table('devices')
        errors = client.insert_rows_json(
            table=target_table,
            json_rows=rows,
        )
        if errors:
            print(errors)
            return False
        else:
            return True


def addNewDevices(devices_this_call:set):
    global logged_devices

    # Update our list of (global) logged_devices if necessary
    getLoggedDevices()

    # Figure out which devices from this invocation have never been seen by our DB
    new_devices = devices_this_call - logged_devices

    # Add new devices to our meta.devices table
    if addNewDevicesToBigQuery(new_devices):
        
        # Add new devices to (global) logged_devices
        logged_devices = logged_devices.union(new_devices)


def main(data, context):
    query = f"""
    SELECT
        time AS Timestamp, 
        ID AS DeviceID,
        ST_GEOGPOINT(Longitude, Latitude) GPS,
        Altitude AS Elevation,
        CASE 
            WHEN PM1 = -1 THEN NULL
            ELSE PM1
        END PM1,
        CASE 
            WHEN PM2_5 = -1 THEN NULL
            ELSE PM2_5
        END PM2_5,
        CASE 
            WHEN PM10 = -1 THEN NULL
            ELSE PM10
        END PM10,
        CASE 
            WHEN Temperature = -1000 THEN NULL
            ELSE Temperature
        END Temperature, 
        CASE 
            WHEN Humidity = -1000 THEN NULL
            ELSE Humidity
        END Humidity,
        `CO` AS MicsRED,
        `NO` AS MicsNOX,
        MICS AS MicsHeater,
        "AQ&U" AS Source,
        "{getenv('FIELD_LABEL')}" AS Label
    FROM 
        `aqandu-184820.production.airu_stationary` 
    WHERE 
        time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP() , INTERVAL 150 SECOND)
        AND 
        Latitude != 0
    """

    target_table = client.dataset(getenv("BQ_DATASET")).table(getenv("BQ_TABLE"))

    job = client.query(query)
    res = job.result()

    data = []
    devices = set()
    for r in res:
        d = dict(r)
        d['Timestamp'] = str(d['Timestamp'])
        if d['MicsHeater'] is not None:
            d['MicsHeater'] = bool(d['MicsHeater'])
        data.append(d)
        devices.add(d['DeviceID'])

    row_ids = list(map(_hash, data))
    errors = client.insert_rows_json(
        table=target_table,
        json_rows=data,
        row_ids=row_ids,
    )
    if errors:
        print(errors)
    else:
        addNewDevices(devices)


if __name__ == '__main__':
    import os
    os.environ['BQ_TABLE'] = "slc_ut"
    os.environ['BQ_DATASET'] = "telemetry"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../../local/tetrad.json'
    main('data', 'context')
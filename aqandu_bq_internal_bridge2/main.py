from google.cloud.bigquery import Client
from hashlib import md5
from os import getenv
import datetime


def _hash(x):
    return md5(str(x).encode('utf-8')).hexdigest()


def main(data, context):
    query = """
    SELECT
        time AS Timestamp, 
        ID AS DeviceID,
        Latitude,
        Longitude,
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
            WHEN Temperature = -1 THEN NULL
            ELSE Temperature
        END Temperature, 
        CASE 
            WHEN Humidity = -1 THEN NULL
            ELSE Humidity
        END Humidity,
        `CO` AS MicsRED,
        `NO` AS MicsNOX,
        MICS AS MicsHeater
    FROM 
        `aqandu-184820.production.airu_stationary` 
    WHERE 
        time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP() , INTERVAL 150 SECOND)
        AND 
        Latitude != 0
    """
    
    geo = geojson.Point((row[getenv('FIELD_LON')], row[getenv('FIELD_LAT')]))

    client = Client()
    target_table = client.dataset(getenv("BQ_DATASET")).table(getenv("BQ_TABLE"))

    job = client.query(query)
    res = job.result()

    data = []
    for r in res:
        d = dict(r)
        d["Timestamp"] = str(d['Timestamp'])
        if d['MicsHeater'] is not None:
            d['MicsHeater'] = bool(d['MicsHeater'])
        d['Flags'] = 2
        data.append(d)

    row_ids = list(map(_hash, data))
    errors = client.insert_rows_json(
        table=target_table,
        json_rows=data,
        row_ids=row_ids,
    )
    if errors:
        print(errors)
    else:
        print(f"Inserted {len(row_ids)} rows")


if __name__ == '__main__':
    import os
    os.environ['BQ_TABLE'] = "slc_ut"
    os.environ['BQ_DATASET'] = "telemetry"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../../local/tetrad.json'
    main('data', 'context')
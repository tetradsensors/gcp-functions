from google.cloud.bigquery import Client
from os import getenv
from json import dumps

QUERY = f"""
SELECT * EXCEPT(ROW_NUM) FROM (
    SELECT 
        {getenv('FIELD_ID')},
        {getenv('FIELD_PM25')},
        ST_X({getenv('FIELD_GPS')}) as {getenv('FIELD_LON')},
        ST_Y({getenv('FIELD_GPS')}) as {getenv('FIELD_LAT')},
        ROW_NUMBER()
    OVER 
        (PARTITION BY {getenv('FIELD_ID')}) AS ROW_NUM
    FROM 
        `{getenv('BQ_TABLE')}` 
    WHERE 
        {getenv('FIELD_TS')} > TIMESTAMP_SUB(CURRENT_TIMESTAMP() , INTERVAL 15 MINUTE)
        AND
        (
            {getenv('FIELD_SRC')} = "{getenv('SRC_TETRAD')}"
            OR
            {getenv('FIELD_SRC')} = "{getenv('SRC_AQU')}"
        )
)
WHERE 
    ROW_NUM = 1
"""

client = Client()

def main(request):
    """HTTP Cloud Function.
    Args:
        N/A
    Returns:
        Last measurement for each sensor in Tetrad or AQ&U.
        Uses data from last 15 minutes. 
    """
    
    job = client.query(QUERY)
    rows = job.result()
    rows = [dict(r) for r in rows]
    response = dumps(rows)
    return response
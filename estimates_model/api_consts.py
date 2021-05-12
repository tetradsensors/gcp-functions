from os import getenv 


# # Get env variables
# PROJECT_ID = getenv("GOOGLE_CLOUD_PROJECT")
# BQ_DATASET_TELEMETRY = getenv("BQ_DATASET_TELEMETRY") 
# BQ_TABLE_SLC    = getenv("BQ_TABLE_SLC")
# BQ_TABLE_CHATT  = getenv("BQ_TABLE_CHATT")
# BQ_TABLE_CLEV   = getenv("BQ_TABLE_CLEV")
# BQ_TABLE_KC     = getenv("BQ_TABLE_KC")
# BQ_TABLE_BADGPS = getenv("BQ_TABLE_BADGPS")
# BQ_TABLE_GLOBAL = getenv("BQ_TABLE_GLOBAL")

# SPACE_KERNEL_FACTOR_PADDING = float(getenv("SPACE_KERNEL_FACTOR_PADDING"))
# TIME_KERNEL_FACTOR_PADDING = float(getenv("TIME_KERNEL_FACTOR_PADDING"))

# LAT_SIZE = float(getenv("LAT_SIZE"))
# LON_SIZE = float(getenv("LON_SIZE"))

# # Table sources as they appear in route arguments
# # (the keys) and their mapping to BigQuery table names
# SRC_MAP = {
#     getenv("Q_SLC"):    BQ_TABLE_SLC,
#     getenv("Q_CHATT"):  BQ_TABLE_CHATT,
#     getenv("Q_CLEV"):   BQ_TABLE_CLEV,
#     getenv("Q_KC"):     BQ_TABLE_KC,
#     getenv("Q_BADGPS"): BQ_TABLE_BADGPS,
#     getenv("Q_GLOBAL"): BQ_TABLE_GLOBAL,
#     getenv("Q_ALL"):    None,
#     getenv("Q_ALLGPS"): None,
# }

# ALLGPS_TBLS = {
#     getenv("Q_SLC"):    BQ_TABLE_SLC,
#     getenv("Q_CHATT"):  BQ_TABLE_CHATT,
#     getenv("Q_CLEV"):   BQ_TABLE_CLEV,
#     getenv("Q_KC"):     BQ_TABLE_KC,
#     getenv("Q_GLOBAL"): BQ_TABLE_GLOBAL,
# }

# ELEV_MAPS = {
#     getenv("Q_SLC"):   getenv("ELEV_MAP_SLC_FILENAME"),
#     getenv("Q_CHATT"): getenv("ELEV_MAP_CHATT_FILENAME"),
#     getenv("Q_CLEV"):  getenv("ELEV_MAP_CLEV_FILENAME"),
#     getenv("Q_KC"):    getenv("ELEV_MAP_KC_FILENAME"),
# }

# # Field names as they appear in route arguments
# #   and their mapping to BigQuery field names
FIELD_MAP = {
    getenv("Q_TS"):   getenv("FIELD_TS"),
    getenv("Q_ID"):   getenv("FIELD_ID"),
    getenv("Q_LAT"):  getenv("FIELD_LAT"),
    getenv("Q_LON"):  getenv("FIELD_LON"),
    getenv("Q_ELE"):  getenv("FIELD_ELE"),
    getenv("Q_PM1"):  getenv("FIELD_PM1"),
    getenv("Q_PM2"):  getenv("FIELD_PM2"),
    getenv("Q_PM10"): getenv("FIELD_PM10"),
    getenv("Q_TEMP"): getenv("FIELD_TEMP"),
    getenv("Q_HUM"):  getenv("FIELD_HUM"),
    getenv("Q_RED"):  getenv("FIELD_RED"),
    getenv("Q_NOX"):  getenv("FIELD_NOX"),
    getenv("Q_HTR"):  getenv("FIELD_HTR"),
}

# # The query-able fields
# VALID_QUERY_FIELDS = {
#     getenv("Q_ELE"):  getenv("FIELD_ELE"),
#     getenv("Q_PM1"):  getenv("FIELD_PM1"),
#     getenv("Q_PM2"):  getenv("FIELD_PM2"),
#     getenv("Q_PM10"): getenv("FIELD_PM10"),
#     getenv("Q_TEMP"): getenv("FIELD_TEMP"),
#     getenv("Q_HUM"):  getenv("FIELD_HUM"),
#     getenv("Q_RED"):  getenv("FIELD_RED"),
#     getenv("Q_NOX"):  getenv("FIELD_NOX"),
#     getenv("Q_HTR"):  getenv("FIELD_HTR"),
# }
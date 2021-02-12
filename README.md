# GCP Functions
All of the GCP functions are in this repo. 
- `aqandu_bq_bridge`: Move data from AQ&U DB to Tetrad DB using AQ&U public API.
- `aqandu_bq_internal_bridge`: Move data form AQ&U DB to Tetrad DB using internal AQ&U BigQuery permissions. Obtained by contacting AQ&U development team. 
- `model_matrix_bridge`: Compute interpolated pollution data for the specified cities and save the matrices in FireStore. Fires every 15 minutes. 
- `ps_bq_bridge`: "PubSub BigQuery Bridge". Accepts MQTT PubSub data from the deployed Tetrad sensors and forwards the packet of data to Tetrad BigQuery Database. 

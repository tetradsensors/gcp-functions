### GCP Functions
All of the GCP functions are in this repo. 
- `aqandu_bq_bridge`: Move data from AQ&U DB to Tetrad DB using AQ&U public API.
- `aqandu_bq_internal_bridge`: Move data form AQ&U DB to Tetrad DB using internal AQ&U BigQuery permissions. Obtained by contacting AQ&U development team. 
- `model_matrix_bridge`: Compute interpolated pollution data for the specified cities and save the matrices in FireStore. Fires every 15 minutes. 
- `ps_bq_bridge`: "PubSub BigQuery Bridge". Accepts MQTT PubSub data from the deployed Tetrad sensors and forwards the packet of data to Tetrad BigQuery Database. 

### General Cloud Functions Notes
* The same `.env.yaml` file is used for all GCP functions and App Engine `backend` instance. The latest copy is stored in Cloud Storage. Please upload/download the latest to this location so that it can be shared across all projects and scripts. 
* ***Performance Enhancements*** Globals and lazy globals are important to the active time of the function. Since we're charged based off of time used it's important to get this low. If your script uses globals they will be persistent across instantiations, until the instantiation closes (after 15 minutes of inactivity). So if you have something that takes a long time to load, like calling a server or loading a client, if you do it as a global and the Function gets called more than once per 15 minutes that variable won't get loaded again. Likewise, lazy globals are something like `my_global = None`. Then you can instantiate it inside a function. If that function doesn't get called every instance then you don't need to waste time waiting for that variable to get loaded. Every ms matters. Here are some articles about globals:
  - [App Engine Startup Time and the Global Variable Problem](https://medium.com/google-cloud/app-engine-startup-time-and-the-global-variable-problem-7ab10de1f349)
  - [Google Cloud Functions Best Practices - Performance](https://cloud.google.com/functions/docs/bestpractices/tips#performance)
  - [Google Cloud Functions Best Practices - Globals](https://cloud.google.com/functions/docs/bestpractices/tips#use_global_variables_to_reuse_objects_in_future_invocations)
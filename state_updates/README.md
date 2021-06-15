#### Google Cloud Functions Deploy:
```bash
gcloud functions deploy airu_connector --runtime python38 --trigger-topic telemetry --entry-point=main --env-vars-file .env.yaml
```

#### Notes
* Globals: clients, bounding box file from Storage, new devices. Since this gets called more than once per 15 minutes, client will stay loaded between instantiations
* New devices part: A BigQuery table `meta.devices` is a list of devices that have published data to the `telemetry.telemetry` table. Keep this loaded and add to it when a new device pops up. This is also responsible for checking when an AQ&U sensor gets refurbished by Tetrad and is now a `AQ&U/Tetrad` device, so this function is responsible for updating the sensor `Source` in `meta.devices`
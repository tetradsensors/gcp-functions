This directory holds files for a GCP Function to query data from `aqandu/api/liveSensors` route and add them to our database:

1. A Google Cloud Scheduler Job (`get_aqandu_airu`) fires periodically, which creates a `pubsub` event message on the topic `trigger_get_aqandu_airu`
2. A Google Cloud Function waits for a `pubsub` publishing event on the aforementioned topic, which triggers a python script (`main.py`).
3. The python script calls the AQ&U API route (`/api/liveSensors`), which returns a JSON object of sensor data
4. The data is sent to the BigQuery table `slc_ut`

Here are the gcloud commands used to deploy the Function and Scheduler job:

Deploy Scheduler Job:
```bash
gcloud scheduler jobs create pubsub aqandu_bq_bridge --schedule "*/1 * * * *" --topic trigger_aqandu_bq_bridge --message-body "PewPew"
```
Deploy Function:
```bash
gcloud functions deploy aqandu_bq_bridge --entry-point main --runtime python38 --trigger-resource trigger_aqandu_bq_bridge --trigger-event google.pubsub.topic.publish --timeout 540s --env-vars-file .env.yaml
```

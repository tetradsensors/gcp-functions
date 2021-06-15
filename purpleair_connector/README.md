## AQ&U BigQuery Bridge (Internal)
This is a bridge between Purple Air and Tetrad BigQuery.

1. A Google Cloud Scheduler Job (`purpleair_connector`) fires periodically (every 5 minutes), which creates a `pubsub` event message on the topic `trigger_purpleair_connector`
2. Query https://www.purpleair.com/json?a to get last 6 minutes of data
4. The data is sent to the BigQuery table `telemetry.telemetry`

Here are the gcloud commands used to deploy the Function and Scheduler job:

Deploy Scheduler Job:
```bash
gcloud scheduler jobs create pubsub purpleair_connector --schedule "*/5 * * * *" --topic trigger_purpleair_connector --message-body "PewPew"
```
Deploy Function:
```bash
gcloud functions deploy purpleair_connector --entry-point main --runtime python38 --trigger-resource trigger_purpleair_connector --trigger-event google.pubsub.topic.publish --timeout 540s --env-vars-file .env.yaml --memory 512
```
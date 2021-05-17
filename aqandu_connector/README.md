## AQ&U BigQuery Bridge (Internal)
This is a bridge between AQ&U BigQuery and Tetrad BigQuery. It operates interally until AQ&U has a proper API setup for Tetrad to query publically. 

1. A Google Cloud Scheduler Job (`aqandu_connector`) fires periodically (every 2 minutes), which creates a `pubsub` event message on the topic `trigger_aqandu_connector`
2. BigQuery query to `aqandu-184820.production.airu_stationary` to get last 150 seconds of data
4. The data is sent to the BigQuery table `telemetry.telemetry`

Here are the gcloud commands used to deploy the Function and Scheduler job:

Deploy Scheduler Job:
```bash
gcloud scheduler jobs create pubsub aqandu_connector --schedule "*/2 * * * *" --topic trigger_aqandu_connector --message-body "PewPew"
```
Deploy Function:
```bash
gcloud functions deploy aqandu_connector --entry-point main --runtime python38 --trigger-resource trigger_aqandu_connector --trigger-event google.pubsub.topic.publish --timeout 20s --env-vars-file .env.yaml
```
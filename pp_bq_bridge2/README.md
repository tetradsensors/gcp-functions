## AQ&U BigQuery Bridge (Internal)
This is a bridge between AQ&U BigQuery and Tetrad BigQuery. It operates interally until AQ&U has a proper API setup for Tetrad to query publically. 

1. A Google Cloud Scheduler Job (`pp_bq_bridge2`) fires periodically (every 2 minutes), which creates a `pubsub` event message on the topic `trigger_pp_bq_bridge2`
2. Query https://www.purpleair.com/json?a to get last 6 minutes of data
4. The data is sent to the BigQuery table `dev.telemetry`

Here are the gcloud commands used to deploy the Function and Scheduler job:

Deploy Scheduler Job:
```bash
gcloud scheduler jobs create pubsub pp_bq_bridge2 --schedule "*/5 * * * *" --topic trigger_pp_bq_bridge2 --message-body "PewPew"
```
Deploy Function:
```bash
gcloud functions deploy pp_bq_bridge2 --entry-point main --runtime python38 --trigger-resource trigger_pp_bq_bridge2 --trigger-event google.pubsub.topic.publish --timeout 540s --env-vars-file .env.yaml
```
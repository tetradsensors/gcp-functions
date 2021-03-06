This directory holds files for a GCP Function to get a model PM matrix and store it in Firebase, where it can be used by the front end webpage for graphic display purposes. The flow is as follows:

1. A Google Cloud Scheduler Job (`estimates`) fires periodically, which creates a `pubsub` event message on the topic `trigger_estimates`
2. A Google Cloud Function waits for a `pubsub` publishing event on the aforementioned topic, which triggers a python script (`main.py`).
3. The python script collects data from BigQuery and computes a rectangular model of interpolated PM2.5 values for the given timestamp. It then stores this 2D matrix in Firestore

Here are the gcloud commands used to deploy the Function and Scheduler job:

Deploy Scheduler Job:
```bash
gcloud scheduler jobs create pubsub estimates --schedule "*/15 * * * *" --topic trigger_estimates --message-body " "
```
Deploy Function:
```bash
gcloud functions deploy estimates --entry-point main --runtime python38 --trigger-resource trigger_estimates --trigger-event google.pubsub.topic.publish --timeout 540s --memory 4096
```
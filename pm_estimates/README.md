Deploy Function:
```bash
gcloud functions deploy pm_estimates --entry-point main --runtime python38 --trigger-resource trigger_pm_estimates --trigger-event google.pubsub.topic.publish --timeout 540s --memory 4096
```
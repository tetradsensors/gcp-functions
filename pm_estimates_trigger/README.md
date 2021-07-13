Deploy Scheduler Job:
```bash
gcloud scheduler jobs create pubsub pm_estimates --schedule "*/15 * * * *" --topic trigger_trigger_pm_estimates --message-body " "
```
Deploy Function:
```bash
gcloud functions deploy pm_estimates_trigger --entry-point main --runtime python38 --trigger-resource trigger_trigger_pm_estimates --trigger-event google.pubsub.topic.publish --timeout 5s
```
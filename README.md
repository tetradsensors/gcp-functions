GCP Functions deploy command:
```bash
gcloud functions deploy ps_bq_bridge \
--retry \
--region us-central1 \
--trigger-topic telemetry \
--runtime python38 \
--ignore-file .gcloudignore \
--entry-point ps_bq_bridge \
--env-vars-file .env.yaml
```
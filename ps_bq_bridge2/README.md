#### Google Cloud Functions Deploy:
```bash
gcloud functions deploy ps_bq_bridge2 \
--runtime python38 \
--trigger-topic telemetry \
--entry-point=main  \
--env-vars-file .env.yaml
```
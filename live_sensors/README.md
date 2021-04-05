### Deploy command:
```bash
gcloud functions deploy live_sensors --runtime python38 --trigger-http --entry-point main --env-vars-file .env.yaml
```

### To call it:
```bash
curl -X GET "https://us-central1-tetrad-296715.cloudfunctions.net/live_sensors" -H "Content-Type:application/json" -H "Authorization: bearer $(gcloud auth print-identity-token --impersonate-service-account=tetrad-296715@appspot.gserviceaccount.com)"
```
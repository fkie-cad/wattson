# Import / Export Kibana Dashboards


## Export Dashboards

1. Create dashboard in kibana and save it in ES index.
2. Download dashboard with curl
```shell
curl -X POST <elk-container-ip>:<kibana-port>/api/saved_objects/_export -o dashboard.ndjson -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d '
{
  "objects": [
    {
      "type": "dashboard",
      "id": "<dashboard-id>"
    }
  ]
}'
```
The dashboard id can be found in the URL when the dashboard is open in kibana.

# Import Dashboards

1. Upload dashboard to ES index
```shell
curl -X POST <elk-container-ip>:<kibana-port>/api/saved_objects/_import?createNewCopies=true -H "kbn-xsrf: true" --form file=@dashboard.ndjson
```
2. Open dashboard in kibana

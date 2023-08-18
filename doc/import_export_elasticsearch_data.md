# Import / Export Elasticsearch Data

We need `elasticdump` to import export data to ES.

# Export Data

```shell
elasticdump \
> --input=http://<elastic-container-ip>:<elastic-port>/*/ \
> --output=data.json \
> --type=data
```

```shell
elasticdump \
> --input=http://<elastic-container-ip>:<elastic-port>/*/ \
> --output=mapping.json \
> --type=mapping
```


# Import Data


```shell
elasticdump \
> --input=data.json \
> --output=http://<elastic-container-ip>:<elastic-port>/*/ \
> --type=data
```

```shell
elasticdump \
> --input=mapping.json \
> --output=http://<elastic-container-ip>:<elastic-port>/*/ \
> --type=mapping
```
## Modified the ingest script
It will now also ingest zones that is given by an extra param `--url_zones`.

## In order to run as pipeline:

build the container:

`docker build -t taxi_ingest:v001 .`

compose PGadmin and database

`docker-compose up -d`

then check the network it created by 

`docker network ls`

copy the network ID

Run the below command

`docker run -it \
    --network=<network_id_here> \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi_ninke \
    --table_name=green_taxi_trips \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz" \
    --url_zones="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"`
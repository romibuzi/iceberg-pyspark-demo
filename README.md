Iceberg PySpark demo
======

Demo of how to use Apache Iceberg table format from a PySpark application.

## Requirements

- Docker Compose
- IDE or Text Editor

## Steps

MinIO object storage, PostgreSQL server and PySpark application are managed through `docker-compose`, 
you can start the project with `up` command:

```
docker-compose up -d
```

- Minio admin console will be accessible at http://127.0.0.1:9001. login: `minioadmin`. password: `minioadmin`.
- PostgreSQL server will be available at 127.0.0.1:5432 (`postgres`/`postgres`). Database is `iceberg_db`.
- PySpark application will create the Iceberg table and then terminate
 
You can edit [main.py](iceberg-pyspark/main.py) and uncomment methods you want in this file to write/read or edit the Iceberg table.

To relaunch PySpark application after modifications in the code:

```
docker-compose run --rm pyspark_app
```

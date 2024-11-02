### Initial environment set up

```bash
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

```bash
docker-compose build
docker-compose up airflow-init
docker-compose up
```

After updates to a DAG, it may take a while before your DAG appears in the UI. If needed, run the following command:

```bash
docker exec -it --user airflow airflow-scheduler bash -c "airflow dags list"
```
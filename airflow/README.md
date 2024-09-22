mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" >> .env

create requirements.txt


Create a Dockerfile pointing to Airflow version 2.9.3 (this worked with ARM), for the base image,

And customize this Dockerfile by:

Adding your custom packages to be installed (OCI)


https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2022/week_2_data_ingestion/airflow/docker-compose-nofrills.yml

https://github.com/marclamberti/docker-airflow/blob/main/docker-compose.yml

https://medium.com/@sunil.veera/001-installing-apache-airflow-a-beginners-guide-76265a30faf0


https://towardsdatascience.com/setting-up-apache-airflow-with-docker-compose-in-5-minutes-56a1110f4122



Note: It may take a while before your DAG appears in the UI. We can speed things up by running the following command in our terminal 
docker exec -it --user airflow airflow-scheduler bash -c "airflow dags list"

example using airflow-scheduler id:
docker exec -it --user airflow 343ef61e8f50 bash -c "airflow dags list"
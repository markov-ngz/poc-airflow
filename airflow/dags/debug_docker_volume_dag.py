from airflow.decorators import task, dag 
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount


@dag(start_date=datetime(2025,9,1), schedule="@daily", catchup=False, tags=["tutorial"],)
def debug_docker_volume_dag():

    task_1 = DockerOperator(
        task_id='task_1',
        image='python:3.13-slim',
        command='ls dbt_project/dbt_project/<repository dbt project path>',
        docker_url="unix://var/run/docker.sock", # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode="airflow_docker_default",
        auto_remove='success',
        mounts=[Mount(source="dbt_project_volume",target="/dbt_project/")]
    )

    task_1


debug_docker_volume_dag()
from airflow.decorators import task, dag 
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

@dag(start_date=datetime(2025,9,1), schedule="@daily", catchup=False, tags=["tutorial"],)
def debug_docker_dag():
    @task
    def dummy_task():
        return "Task completed"

    t1 = dummy_task()

    
    t2 = DockerOperator(
        task_id='t2',
        image='python:3.13-slim',
        command='echo "command running in the docker container"',
        docker_url="unix://var/run/docker.sock", # as airflow is run inside docker, the socket must be mounted inside the container so that it can listen to docker daemon 
        network_mode="airflow_docker_default",
        auto_remove='success',
    )
    t1 >> t2 

    

debug_docker_dag()
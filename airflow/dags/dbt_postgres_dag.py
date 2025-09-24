from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
from airflow import DAG
from airflow.sdk import Variable

with DAG(
    dag_id="dbt_postgres_dag",
    start_date=datetime(2022, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["tutorial"],
) as dag:
    
    DBT_PROJECT_PATH = "/dbt_project/dbt_project/{0}".format(Variable.get("DBT_PROJECT_PATH")) 
    PG_HOST=Variable.get("PG_HOST")
    PG_SCHEMA=Variable.get("PG_SCHEMA")
    PG_USER=Variable.get("PG_USER")
    PG_PASSWORD=Variable.get("PG_PASSWORD")

    dbt_list = DockerOperator(
        task_id='dbt_list',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.0',
        command='list --log-format json',
        docker_url="unix://var/run/docker.sock", 
        network_mode="docker_default",
        auto_remove='success',
        mounts=[Mount(source="dbt_project_volume",target="/dbt_project/",type="volume")],
        dag=dag,
        private_environment={
                        "PG_HOST":PG_HOST,
                        "PG_PASSWORD":PG_PASSWORD,
                        "PG_SCHEMA":PG_SCHEMA,
                        "PG_USER":PG_USER
                    },
        environment={
            "DBT_PROJECT_DIR":DBT_PROJECT_PATH,
            "DBT_PROFILES_DIR":DBT_PROJECT_PATH
        },
        xcom_all=True,
        mount_tmp_dir=False
    )





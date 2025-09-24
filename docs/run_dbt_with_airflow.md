# How to run DBT with Airflow ? 

## Available options
- BashOperator
- DockerOperator or KubernetesPodOperator
- DbtRunOperator (https://github.com/gocardless/airflow-dbt)
- PythonOperator 

## Needs
- Easily parse logs
- Load and Save manifest.json
- Save run_result.json
- Parametrize commands to only run a subset of models
- Lightweight run is a bonus 

## Resume 

| Option                          |  Notes | Difficulty | Personal Preference|
|---------------------------------|------|----|----|
| **BashOperator**                 | Worker needs dependencies and project mounted any changes needs to update infra   |2 |  |
| **DockerOperator**               | Scalable but fetching logs and files needs some Volume mastering level  | 3 | ⭐⭐ |
| **KubernetesPodOperator**        | Scalable but fetching logs and files needs some Volume level  even more on k8s  which is a technical gap ppl can't afford to fill  | 5 | |
| **DbtRunOperator (airflow-dbt)** | Seems like the same as BashOperator but clean  interface   | 2 | |
| **PythonOperator**               |  Worker needs dependencies and project mounted  => any changes needs to update infra  | 2 | |

[modulo] Airflow deployment environment : k8s, docker, saas version ? 

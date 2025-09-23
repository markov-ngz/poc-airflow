## Airflow Local Setup

## 1. Setup de Airflow
Setup les variables d'environnements nécessaires
```
airflow.env.bat
```
Lancer Airflow
(doc : https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html )
```
# 1. Database Migration
docker compose  -f airflow.docker-compose.yaml up airflow-init

# 2. Run the whole thing
docker compose -f airflow.docker-compose.yaml up -d
```

custom build image avec DockerOperator et docker compose modifié pour donner accès à la socket docker de la machine host 

Une fois les commandes passées:
- aller à localhost:8080
- User + pwd admin : airflow:airflow


## 2. Synchronisation  du projet DBT  à un repo distant

Setup les variables d'environnements nécessaires
```
git-sync-dbt-project.env.bat
```
Lancer le conteneur pour populer le volume
```
docker compose -f git-sync-dbt-project.docker-compose.yaml up -d
```
On utilisera la meme technique de mise à jour en production pour synchroniser les dags présents dans un répo git afin de les monter dans un volume au lieu de monter de notre répertoire hote.


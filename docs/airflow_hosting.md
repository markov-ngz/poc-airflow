# Hosting Airflow

## Available Options
**On premise:**
- k8s Cluster

**Managed:**
- Astronomer
- Google Cloud Composer (GCP)
- Azure Managed Airflow (Microsoft avric)


## Criteria Ideas 
- How are managed Airflow dependencies like python operators ?
- How are managed workers file directories , is there a Docker Socket ?
- Integration with private VPC
- How is it priced  (worker-hour pricing, hidden fees, storage costs)
- Git integration (auto-deploy, branch-based testing)
- Built-in IDE for non-developers? (visual DAG builders)

## Problems

How are folders mounted for hosted workers ? 
Is there a way to set default dependencies to some workers ? And specify which workers run for which tasks ? 
For bash operator + python operator, is it kept the same directory ?


## Needs 

k8s / docker notions when developing dags


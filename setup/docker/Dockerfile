FROM apache/airflow:3.0.6

# No need to change user, as requirements needs to be installed as 'Airflow' user
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt  --no-cache-dir 
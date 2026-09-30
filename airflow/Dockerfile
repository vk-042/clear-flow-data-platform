FROM apache/airflow:2.10.5-python3.11
COPY --chown=airflow:root src /opt/clearflow/src
COPY --chown=airflow:root airflow/dags /opt/clearflow/dags
# Airflow 2 uses SQLAlchemy 1.4; shared core uses APIs compatible with both 1.4 and 2.0.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "pydantic==2.11.9" "psycopg2-binary==2.9.10"
ENV PYTHONPATH=/opt/clearflow/src AIRFLOW__CORE__DAGS_FOLDER=/opt/clearflow/dags

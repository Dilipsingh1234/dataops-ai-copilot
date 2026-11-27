FROM apache/airflow:3.4.0-python3.12@sha256:818f94affc5bd7ce0be0cc04f2ace09aa8382e65190a73a08881229ff32700cb

# The base image already supplies all third-party ingestion dependencies.
RUN python -c "import dotenv, psycopg, sqlalchemy"

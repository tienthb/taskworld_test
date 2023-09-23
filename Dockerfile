FROM python:3.8-slim

RUN apt-get update && apt-get install -y libpq-dev postgresql-client gcc 

COPY src/data/ /opt/data/

WORKDIR /src

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .
COPY tests/ ./tests/

# ENTRYPOINT ["python3", "main.py", "--source",  "/opt/data/activity.csv", "--database", "warehouse", "--table", "user_activity"]
ENTRYPOINT ["python3", "-m", "unittest"]
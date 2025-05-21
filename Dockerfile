FROM python:3.12-slim

RUN apt-get update && apt-get install -y git

COPY app /app

RUN pip install -r /app/requirements.txt

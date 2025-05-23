FROM python:3.12-slim

RUN apt-get update && apt-get install -y git

COPY app/requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY app /app

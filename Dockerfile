# syntax=docker/dockerfile:1

FROM python:3.10-slim
WORKDIR /app

COPY . .
RUN pip3 install .

RUN rm -r /app

ENTRYPOINT ["cumulus-etl"]

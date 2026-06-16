# syntax=docker/dockerfile:1

FROM python:3.11 AS cumulus-etl-test
COPY --from=eclipse-temurin:21-jre /opt/java/openjdk /opt/java/openjdk
COPY . /app
RUN pip3 install uv
RUN uv venv
RUN --mount=type=cache,target=/root/.cache \
  uv install /app/[tests]
RUN rm -r /app

ENV JAVA_HOME=/opt/java/openjdk

ENTRYPOINT ["cumulus-etl"]

FROM python:3.11 AS cumulus-etl
COPY --from=eclipse-temurin:21-jre /opt/java/openjdk /opt/java/openjdk

# Ship pre-downloaded nltk files, used by philter
RUN pip3 install uv
RUN uv venv
RUN uv pip install nltk
RUN uv run python3 -m nltk.downloader -d /usr/local/share/nltk_data averaged_perceptron_tagger

COPY . /app

ARG ETL_VERSION
RUN [ -z "$ETL_VERSION" ] || sed -i "s/1\!0\.0\.0/$ETL_VERSION/" /app/cumulus_etl/__init__.py
# Print the final version we're using
RUN grep __version__ /app/cumulus_etl/__init__.py

RUN --mount=type=cache,target=/root/.cache \
  uv pip install /app
RUN rm -r /app

ENV JAVA_HOME=/opt/java/openjdk

ENTRYPOINT ["uv", "run", "cumulus-etl"]

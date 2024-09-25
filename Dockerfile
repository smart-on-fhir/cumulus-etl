# syntax=docker/dockerfile:1

FROM alpine/git AS ms-tool-src
RUN git clone https://github.com/microsoft/Tools-for-Health-Data-Anonymization.git /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS ms-tool
COPY --from=ms-tool-src /app /app
# This will force builds to fail if the environment piping breaks for some reason
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN arch=$(arch | sed s/aarch64/arm64/ | sed s/x86_64/x64/) && \
  dotnet publish \
  --runtime=linux-${arch} \
  --self-contained=true \
  --configuration=Release \
  -p:InvariantGlobalization=true \
  -p:PublishSingleFile=true \
  --output=/bin \
  /app/FHIR/src/Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool

FROM python:3.11 AS cumulus-etl-test
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
COPY --from=ms-tool /bin/Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool /bin
COPY . /app
RUN --mount=type=cache,target=/root/.cache \
  pip3 install /app/[tests]
RUN rm -r /app

ENV JAVA_HOME=/opt/java/openjdk

ENTRYPOINT ["cumulus-etl"]

FROM python:3.11 AS cumulus-etl
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
COPY --from=ms-tool /bin/Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool /bin

# Ship pre-downloaded nltk files, used by philter
RUN pip3 install nltk
RUN python3 -m nltk.downloader -d /usr/local/share/nltk_data averaged_perceptron_tagger

COPY . /app

# A local version is the trailing bit of a Python version after a plus sign, like 5.0+ubuntu1.
# We use it here mostly to inject a git commit sha when building from git.
ARG LOCAL_VERSION
RUN [ -z "$LOCAL_VERSION" ] || sed -i "s/\(__version__.*\)\"/\1+$LOCAL_VERSION\"/" /app/cumulus_etl/__init__.py
# Print the final version we're using
RUN grep __version__ /app/cumulus_etl/__init__.py

RUN --mount=type=cache,target=/root/.cache \
  pip3 install /app
RUN rm -r /app

ENV JAVA_HOME=/opt/java/openjdk

ENTRYPOINT ["cumulus-etl"]

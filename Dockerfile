# syntax=docker/dockerfile:1

FROM alpine/git as ms-tool-src
RUN git clone https://github.com/microsoft/Tools-for-Health-Data-Anonymization.git /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS ms-tool
COPY --from=ms-tool-src /app /app
RUN dotnet publish \
  --runtime=linux-x64 \
  --self-contained=true \
  --configuration=Release \
  -p:InvariantGlobalization=true \
  -p:PublishSingleFile=true \
  --output=/bin \
  /app/FHIR/src/Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool

FROM python:3.10 AS cumulus-etl-test
COPY --from=ms-tool /bin/Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool /bin
COPY . /app
RUN --mount=type=cache,target=/root/.cache \
  pip3 install /app/[tests]
RUN rm -r /app

ENTRYPOINT ["cumulus-etl"]

FROM python:3.10 AS cumulus-etl
COPY --from=ms-tool /bin/Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool /bin
COPY . /app
RUN --mount=type=cache,target=/root/.cache \
  pip3 install /app
RUN rm -r /app

ENTRYPOINT ["cumulus-etl"]

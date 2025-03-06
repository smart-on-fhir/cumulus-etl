#!/bin/sh

set -e

VERSION=$(grep __version__ cumulus_etl/__init__.py | cut -d'"' -f2 | cut -d. -f1)
if [ -z "$VERSION" ]; then
  echo "Something is wrong, couldn't parse __version__."
  exit 1
fi

tag_image() {
  docker buildx imagetools create \
    --tag smartonfhir/cumulus-etl:$1 \
    smartonfhir/cumulus-etl:main
}

tag_image latest
tag_image $VERSION

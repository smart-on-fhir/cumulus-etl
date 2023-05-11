# Maintainers guide

This guide is intended for maintainers of the cumulus-etl project - i.e. anyone with commit access to the repository

We (informally) use [trunk based development](https://trunkbaseddevelopment.com/) - so PRs are generally targeted towards small feature sets and are rolled out incrementally. 

## Pull requests

Public discussion of pull requests is strongly encouraged for transparency. For anything with a potential user impact, we also encourage notifying members in the #dev channel in the smart-cumulus slack.

## Running tests

There are two ways to run tests:

### Local

To run unit tests directly on your machine, you will need to install a copy of the Microsoft De-ID tool. We recommend mimicing the [steps in the cumulus dockerfile](https://github.com/smart-on-fhir/cumulus-etl/blob/main/Dockerfile#L4-L15) related to this tooling, and add it to your system PATH, or symlink the executable into someplace that is already on your system path.

Tests are run with `pytest [optional args]`, and coverage is run with `coverage run -m pytest [optional args] && coverage report -m`

### In container

We have a container, `cumulus-etl-test`, which can be leveraged to run tests. To run these tests, use `docker compose run --entrypoint "pytest [optional args]" --rm cumulus-etl-test`, and to run coverage, use `docker compose run --entrypoint "coverage run -m pytest [optional args]` followed by `coverage report -m" --rm cumulus-etl-test`

## Release management

Cumulus ETL deployments are currently done directly from source control. Currently the etl container is build on demand, but it will be converted to a Docker image as soon as the project is public. Support infrastructure for the etl container is already managed in this way.

We generate cross platform images using [Docker's buildx](https://docs.docker.com/engine/reference/commandline/buildx/). After authing your local Docker with an account connected to the smartonfhir project on dockerhub, you can run a build and release with the following command:

docker buildx build \
	--platform linux/arm64,linux/arm/v7,linux/arm/v8,linux/amd64 \
	--tag smartonfhir/[IMAGE_NAME]:latest \
	--tag smartonfhir/[IMAGE_NAME]:[MAJOR].[MINOR].[PATCH] \
	--tag smartonfhir/[IMAGE_NAME]:[MAJOR].[MINOR] \
	--tag smartonfhir/[IMAGE_NAME]:[MAJOR] \
	. --push

It is currently assumed that cumulus will be spun up on premesis from source control using the project compose.yaml every time the pipeline is run, so release management is just a matter of updating the targeted version in the compose file, if required. Sites electing to use a container management/deploy tool may design other workflows leveraging the available containers.

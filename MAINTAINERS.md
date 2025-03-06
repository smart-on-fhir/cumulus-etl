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

Every commit will kick off a GitHub workflow that builds a multi-platform Docker image
with a development tag of `main` (the branch name).

But users will usually be pulling from the stable `latest` tag.
In fact, the `compose.yaml` file is set to auto-pull from that tag every run.

To mark a `main` Docker build as stable, run `./tag-release.sh`.
This will add the tags `latest` and the current major version (e.g. `2`).

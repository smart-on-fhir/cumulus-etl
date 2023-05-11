---
title: Reviewing Output
parent: ETL
nav_order: 8
# audience: engineer familiar with the project
# type: howto
---

# How To Manually Review the ETL Output

Your organization may require a manual review of all ETL output before uploading to the AWS cloud.

That can be supported easily enough with a two-step ETL process.

## First ETL Step: Generate Human-Readable Files

Follow the [normal ETL flow](setup/sample-runs.md), but:
- Make sure to pass `--output-format=ndjson` to `cumulus-etl`
- Use a local output folder (we don't want this data in the cloud until we've reviewed it)
  - Remember that Docker will require that local folder to be mapped outside of its container
    with something like `--volume /outside/path:/inside/path`

This will drop all ETL results as ndjson files in the target folder.

## Manual Review

These ndjson files are human-readable (though not entirely pleasant) and/or
can be processed with standard json tools.

Whatever your organization's process is, once you are happy with the files,
we can upload these files into a Delta Lake in the cloud.

## Second ETL Step: Upload Binary Files

We want to convert the local ndjson folders into binary Delta Lake files in your AWS cloud.

Thankfully, there's a special `convert` command for that:
```sh
docker compose -f $CUMULUS_REPO_PATH/compose.yaml \
  run --volume /path/to/output:/output --rm \
  cumulus-etl \
  convert \
  --s3-region=us-east-2 \
  /output \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir1/
```

This will copy over the job logs in the `JobConfig` file too.

For help on any other flags or options, pass `--help`.
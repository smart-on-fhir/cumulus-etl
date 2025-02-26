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

Follow the normal ETL flow, but:
- Make sure to pass `--output-format=ndjson` to `cumulus-etl`.
  This will drop all ETL results as NDJSON files in the target folder.
- Use a local output folder (we don't want this data in the cloud until we've reviewed it)
  - Remember that Docker will require that local folder to be mapped outside its container
    with something like `--volume /outside/path:/inside/path`
  - That local output folder must be empty - so follow the full manual review flow for every ETL
    action (e.g. if you run `cumulus-etl init`, review it, and convert it, then when you want
    to run `cumulus-etl etl`, run it in a fresh folder, review it, and convert it)

## Manual Review

These NDJSON files are human-readable (though not entirely pleasant) and/or
can be processed with standard JSON tools.

Whatever your organization's process is, once you are happy with the files,
we can upload these files into a Delta Lake in the cloud.

## Second ETL Step: Upload Binary Files

We want to convert the local NDJSON folders into binary Delta Lake files in your AWS cloud.

Thankfully, there's a special `convert` command for that:
```sh
docker compose run --rm \
  --volume /path/to/ndjson:/host \
  cumulus-etl convert \
  --s3-region=us-east-2 \
  /host \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir/
```

This will copy over the job logs in the `JobConfig` file too.

For help on any other flags or options, pass `--help`.
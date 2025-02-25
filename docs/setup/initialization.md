---
title: ETL Initialization
parent: Production Setup
grand_parent: ETL
nav_order: 2
# audience: engineer familiar with the project
# type: howto
---

# Running Cumulus ETL Against Your New AWS Infrastructure

You may have earlier [run Cumulus ETL locally](../local-setup.md) against some sample data.
Now let's look at a more production-oriented setup in AWS.

This guide will explain how to install and run Cumulus ETL inside your institution's infrastructure.
It assumes you are familiar with the command line.

At the end of this, you'll have some ETL output tables ready to be loaded into AWS Glue.

## On-Premises Runs

But first, a word about on-premises vs in-cloud runs of Cumulus ETL.

Your institution may be more comfortable keeping any semblance of PHI off of AWS.
That's fine! The entire ETL process itself can be run on-premises.

There are two folders that Cumulus ETL generates:
one is the main de-identified output
and the other holds build artifacts (which may include PHI).
Only the de-identified output needs to be put in the cloud.
We'll talk about this more later.

You can follow these steps below on your own desktop or a server you have access to.
Likewise, when you set this up for production use, you can run it on-premises or a cloud machine.

## Prerequisites

- Before we dive in, you're going to want a machine with access to your bulk patient data,
  to run Cumulus ETL.
    * This could be a long-running machine or a disposable per-run cloud instance.
      Whatever works for your setup.
    * If you're running Cumulus ETL in AWS EC2, an `m5.xlarge` instance works well.
    * If you're running on-premises or in another cloud, at least 16GB memory is recommended.
- You should have also already [set up your AWS infrastructure](aws.md)
  (i.e. have S3 buckets and a Glue database ready to go) 

## Installation

Cumulus ETL is shipped as a Docker image driven by a Docker Compose file.

1. Install Docker
   - Follow the [official instructions](https://docs.docker.com/engine/install/)
2. Clone the Cumulus ETL repo, with its Docker Compose file
   - `git clone https://github.com/smart-on-fhir/cumulus-etl.git`
   - `cd cumulus-etl`

The `compose.yaml` file in that directory is all you'll need.
Any further Docker images needed by Cumulus ETL commands will be downloaded on the fly.

Whenever you run Cumulus ETL, this `compose.yaml` file will either need to be in your current
directory, or you'll have to pass Docker Compose the argument `-f /path/to/compose.yaml`.

To keep current with any updates to the Compose file,
you may want to run `git pull` before running Cumulus ETL.

## Initialization

Cumulus ETL needs to initialize the output folder first,
to create all the expected output tables in an empty initial state. 

Run the command below, but replace:
* `my-cumulus-prefix-99999999999-us-east-2` with the output bucket you set up in AWS.
* `subdir` with the ETL subdirectory you used when setting up your AWS buckets
* and `us-east-2` with the AWS region your buckets are in

```sh
docker compose run --rm \
  cumulus-etl init \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir/ \
  --s3-region=us-east-2
```

This will create empty tables for all the core resources that Cumulus works with.

You should now even be able to see some (very small) output files in your S3 buckets!

You can choose to stop here and go back to [setting up AWS Glue & Athena](index.md),
or if you happen to have your FHIR data ready already,
you continue reading on to upload some actual data. 

## Uploading FHIR Data (Optional At This Point)

You'll need some FHIR data from your EHR for this step.
If you don't have it yet, read [more about bulk exporting](../bulk-exports.md)
to learn how to extract your data.

There are three required positional arguments when uploading data:
1. **Input path**: where your FHIR data (as NDJSON files) can be found,
   either as a local folder or an `s3://` URL
1. **Output path**: where to put the de-identified bundles of FHIR resources,
   usually an `s3://` path and usually with a subdirectory specified
1. **PHI build path**: where to put build artifacts that contain PHI and need to persist run-to-run,
   usually a different `s3://` bucket (with less access) or a persistent local folder.
   **Always reuse the same PHI build folder** for the same output folder.

A full Cumulus ETL command looks like:
```sh
docker compose run --rm \
  cumulus-etl etl \
  --s3-region=us-east-2 \
  s3://my-input-bucket/subdir/ \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir/
```

But if you'd prefer to keep PHI off of AWS,
that last argument (the PHI bucket path) can be replaced with a path to persistent local storage.
Just remember to provide a `--volume` argument to Docker for any local paths
you want to map into the container.

(Note: you can always run `docker compose run cumulus-etl --help` to see more options.)

## Next Steps

Go back to [setting up AWS Glue & Athena](index.md).

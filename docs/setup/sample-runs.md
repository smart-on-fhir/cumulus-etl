---
title: Sample Runs
parent: Setup
grand_parent: ETL
nav_order: 2
# audience: engineer familiar with the project
# type: howto
---

# Running Cumulus ETL for the First Time

This guide will explain how to install and run Cumulus ETL inside your hospital's infrastructure.
It assumes you are familiar with the command line.

If you're coming back to these docs after an update to the ETL on a machine you've run it on before,
run `docker pull smartonfhir/cumulus-etl` before you start to get the latest ETL image.

## On-Premises Runs

But first, a word about on-premises vs in-cloud runs of Cumulus ETL.

Your hospital may be more comfortable keeping any semblance of PHI off of AWS.
That's fine! The entire ETL process can be run on-premises.

There are two output folders that Cumulus ETL generates:
one is the main de-identified output
and the other holds build artifacts (which may include PHI).
Only the de-identified output needs to be put in the cloud.
We'll talk about this more later.

You can follow these steps below on your own desktop or a server you have access to.
Likewise, when you set this up for production use, you can run it on-premises or a cloud machine.

## Setup

Before we dive in, you're going to want the following things in place:

1. A server on which to run Cumulus ETL, with access to your bulk patient data
    * This could be a long-running server or a disposable per-run instance.
      Whatever works for your hospital setup.
    * If you're running Cumulus ETL in AWS EC2, an `m5.xlarge` instance works well.
    * If you're running on-premises or in another cloud, at least 16GB memory is recommended.
2. A [UMLS](https://www.nlm.nih.gov/research/umls/index.html) API key
    * Your hospital probably already has one, but they are also easy to
      [request](https://www.nlm.nih.gov/databases/umls.html).

In several places further down in these instructions, we'll reference sample data from the
`./tests/data/simple/input/` folder, which is great for checking if you've got your configuration
correct. However, if you'd like some more realistic data, there are 
[sample bulk FHIR datasets](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets) available
for download - the 100 patient zip file is a good starting point - you can just swap the path
you use for file inputs to point to where you extracted the dataset. 

### Grab the Code

Simply clone the Cumulus ETL repository:
```shell
git clone https://github.com/smart-on-fhir/cumulus-etl.git
```

Further instructions below will pass Docker the argument `-f $CUMULUS_REPO_PATH/compose.yaml`.
You can set that variable like so:
```shell
CUMULUS_REPO_PATH=/path-to-cloned-cumulus-etl-repo
```

Or if you run Docker commands directly from inside the cloned path,
you can drop that `-f` argument entirely.

## Docker

The next few steps will require Docker.
Read the [official Docker installation instructions](https://docs.docker.com/engine/install/)
for help with your specific server platform.

But as an example, here's how you might install it on CentOS:

```sh
yum -y install yum-utils
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
yum -y install docker-ce docker-ce-cli containerd.io docker-scan-plugin docker-compose-plugin docker-ce-rootless-extras
systemctl start docker
systemctl enable docker.service
systemctl enable containerd.service
```

## Docker Compose

[Docker Compose](https://docs.docker.com/compose/) is included with Docker,
and allows you to deploy a self-contained network.
We're using it to simplify deploying the Cumulus ETL project in your ecosystem.

This simplifies starting study-specific services like cTAKES.

As a result, some of the commands below will start with `docker compose` instead of just `docker`.

## Cumulus ETL

### Running Cumulus ETL

At the most basic level, running Cumulus ETL is as simple as `docker compose up`,
but obviously we'll need to provide arguments that point to your data and where output files should
go, etc.

But first, let's do a bare-minimum run that works with toy data, just to confirm that we have
Cumulus ETL installed correctly.

### Local Test Run

Once you've done that, you'll need the UMLS key mentioned at the top of this document.
Let's set the UMLS_API_KEY, so that Cumulus ETL can see it:
```sh
export UMLS_API_KEY=your-umls-api-key
```

The compose file will handle the environment variable mapping and volume mounts for you.
After running that command, you can start the actual etl process with the following command:
```sh
docker compose -f $CUMULUS_REPO_PATH/compose.yaml \
  run --volume $CUMULUS_REPO_PATH:/cumulus-etl --rm \
  cumulus-etl \
  /cumulus-etl/tests/data/simple/input \
  /cumulus-etl/example-output \
  /cumulus-etl/example-phi-build \
  --output-format=ndjson
```

After running this command, you should be able to see output in
`$CUMULUS_REPO_PATH/example-output` and some build artifacts in
`$CUMULUS_REPO_PATH/example-phi-build`. The ndjson flag shows what the data leaving your
organization looks like - take a look if you'd like to confirm that there isn't PHI
in the output directory.

Congratulations! You've run your first Cumulus ETL process. The first of many!

### AWS Test Run

Let's do that again, but now pointing at S3 buckets.
This assumes you've followed the [S3 setup guide](aws.md).

We didn't do this above, but now that we're getting more serious,
let's run `cumulus-etl init` first, which will create all the basic tables for us.

When using S3 buckets, you'll need to set the `--s3-region` argument to the correct region.

Run the command below, but replace:
* `us-east-2` with the region your buckets are in
* `99999999999` with your account ID
* `my-cumulus-prefix` with the bucket prefix you used when setting up AWS
* and `subdir1` with the ETL subdirectory you used when setting up AWS

```sh
docker compose -f $CUMULUS_REPO_PATH/compose.yaml \
  run --rm \
  cumulus-etl init \
  --s3-region=us-east-2 \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir1/
```

This will create empty tables for all the core resources that Cumulus works with.
You should now even be able to see some (very small) output files in your S3 buckets!

Let's go one step further and put some actual (fake) test data in there too.

```sh
docker compose -f $CUMULUS_REPO_PATH/compose.yaml \
  run --volume $CUMULUS_REPO_PATH:/cumulus-etl --rm \
  cumulus-etl \
  --s3-region=us-east-2 \
  /cumulus-etl/tests/data/simple/input \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir1/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir1/
```

(Though, note now that your S3 bucket has test data in it.
Before you put any real data in there, you should delete the S3 folder and start fresh.)

Obviously, this was just example data.
But if you'd prefer to keep PHI off of AWS when you deploy for real,
that last argument (the PHI bucket path) can be replaced with a path to persistent local storage.
Just remember to provide a `--volume` argument for any local paths you want to map into the
container, like we did above for the input folder.

### More Realistic Example Command

Here's a more realistic and complete command, as a starting point for your own version.

```sh
docker compose -f $CUMULUS_REPO_PATH/compose.yaml \
 run --rm \
 cumulus-etl \
  --comment="Any interesting logging data you like, like which user launched this" \
  --input-format=ndjson \
  --output-format=deltalake \
  --batch-size=300000 \
  --s3-region=us-east-2 \
  s3://my-us-east-2-input-bucket/ \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir1/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir1/
```

(Read [more about bulk exporting](../bulk-exports.md)
to learn how to get some real data from your EHR,
and how to properly feed it into the ETL.)

Now let's talk about customizing this command for your own environment.
(And remember that you can always run `docker compose run cumulus-etl --help` for more guidance.)

### Required Arguments

There are three required positional arguments:
1. **Input path**: where your hospital data can be found, see `--input-format` below for more help
   with your options here
1. **Output path**: where to put the de-identified bundles of FHIR resources, usually an `s3://`
   path and usually with a subdirectory specified, to make it easier to do a test run of Cumulus
   ETL in the future, if needed.
1. **PHI build path**: where to put build artifacts that contain PHI and need to persist run-to-run,
   usually a different `s3://` bucket (with less access) or a persistent local folder

When using S3 buckets, you'll need to set the `--s3-region` argument to the correct region.
And you'll want to first actually set up those buckets.
Follow the [S3 setup guide](aws.md) document for guidance there.

### Strongly Recommended Arguments

While technically optional, it's recommended that you manually specify these arguments because their
defaults are subject to change or might not match your situation.

* `--output-format`: There are two reasonable values (`ndjson` and `deltalake`).
  For production use, you can use the default value of `deltalake` as it supports incremental,
  batched updates.
  But since the `ndjson` output is human-readable, it's useful for debugging
  or reviewing the output before pushing to the cloud.

* `--batch-size`: How many resources to save in a single output file. If there are more resources
  (e.g. more patients) than this limit, multiple output files will be created for that resource
  type. The larger the better for performance reasons, but that will also take more memory.
  So this number is highly environment specific. For 16GB of memory, we recommend `300000`.
  
### Optional Arguments

* `--comment`: You can add any comment you like here, it will be saved in a logging folder on the
output path (`JobConfig/`). Might help when debugging an issue.

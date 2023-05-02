<!-- Target audience: engineer familiar with the project, helpful direct tone -->

# How To Run Cumulus ETL at Your Hospital

This guide will explain how to install and run Cumulus ETL inside your hospital's infrastructure.
It assumes you are familiar with the command line.

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
3. A deploy key for the Cumulus ETL repository (see below)

In several places further down in these instructions, we'll reference sample data from the
`./tests/data/simple/input/` folder, which is great for checking if you've got your configuration
correct. However, if you'd like some more realistic data, there are 
[sample bulk FHIR datasets](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets) available
for download - the 1,000 patient zip file is a good starting point - you can just swap the path
you use for file inputs to point to where you extracted the dataset. 

### Access to Cumulus ETL Code

At the time of writing, Cumulus is still not a publicly available project.
You yourself might already have access to the Cumulus ETL repository,
but for installing and running on hospital servers, you probably don't want to use your personal
GitHub credentials.

Instead, you'll use a [GitHub deploy key](https://docs.github.com/en/developers/overview/managing-deploy-keys#deploy-keys).

1. Follow [GitHub's instructions](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key)
to generate a new SSH key for your server.
1. Keep your private key in a safe place (e.g. AWS Secrets Manager).
1. Give the public key to your contact at SMART.
They'll add it to the repository as a valid deploy key.
1. Follow the instructions below for actually pulling the code correctly.

#### Configuring Git Clone

To clone the [Cumulus ETL repository](https://github.com/smart-on-fhir/cumulus-etl),
you'll need to connect over SSH rather than HTTP (to use the SSH deploy key from above).

But often, hospital firewalls block SSH traffic.
Thankfully, GitHub has set up an SSH server on the HTTPS port to accommodate this use case.
Instead of cloning from `github.com` like normal, we'll clone from `ssh.github.com:443`.

Now on to the cloning:

1. Put the private key from above into place.
```sh
cp MY_PRIVATE_KEY ~/.ssh/id_ed25519
chmod 0600 ~/.ssh/id_ed25519 # make sure it has the permissions that ssh requires
```
2. Put the correct SSH fingerprint (for `ssh.github.com:443`) into place.
If you don't do this step, `git` will complain about an unknown server.
```sh
echo '[ssh.github.com]:443 ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=' >> ~/.ssh/known_hosts
```
3. And finally, clone the repository.
```sh
git clone --depth=1 ssh://git@ssh.github.com:443/smart-on-fhir/cumulus-etl.git
```

Now you have the Cumulus ETL code!

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

[Docker Compose](https://docs.docker.com/compose/) is included with Docker, and allows you to deploy a self-contained network.
We're using it to simplify deploying the Cumulus ETL project in your ecosystem.

The docker-compose.yaml, which defines the network, adds the following containers:

- The Cumulus ETL process itself
- A [cTAKES](https://ctakes.apache.org/) server, to handle natural language 
  processing of physician notes.

## Cumulus ETL

### Building a Docker Image
Again, because Cumulus isn't yet a public project, we don't offer a turnkey docker image.
But we _do_ ship a Dockerfile that you can use to build a docker image yourself for now.

You _could_ run Cumulus ETL directly from your cloned repository, but rather than worrying about
all the dependencies it needs (including a whole
[C# app from Microsoft](https://github.com/microsoft/Tools-for-Health-Data-Anonymization)
that does some of the de-identification for us), we'll just build the docker image, using
the Docker Compose network definition.

```sh
CUMULUS_REPO_PATH=/path-to-cloned-cumulus-etl-repo
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker compose -f $CUMULUS_REPO_PATH/compose.yaml --profile etl build
```

And now you have Cumulus ETL installed!

### Running Cumulus ETL

At the most basic level, running Cumulus ETL is as simple as `docker compose up`,
but obviously we'll need to provide arguments that point to your data and where output files should
go, etc.

But first, let's do a bare-minimum run that works with toy data, just to confirm that we have
Cumulus ETL installed correctly.

### Local Test Run

Once you've done that, you'll need the UMLS key mentioned at the top of this document. First, we're going
to start the network (here we're setting the UMLS_API_KEY, which cTAKES requires):
```sh
export UMLS_API_KEY=your-umls-api-key
docker compose -f $CUMULUS_REPO_PATH/compose.yaml --profile etl-support up -d
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
in the output direcotry. 

Congratulations! You've run your first Cumulus ETL process. The first of many!

### AWS Test Run

Let's do the same thing, but now pointing at S3 buckets.
This assumes you've followed the [S3 setup guide](set-up-aws.md).

When using S3 buckets, you'll need to set the `--s3-region` argument to the correct region.

Run this command, but replace:
* `us-east-2` with the region your buckets are in
* `99999999999` with your account ID
* `my-cumulus-prefix` with the bucket prefix you used when setting up AWS
* and `subdir1` with the ETL subdirectory you used when setting up AWS

```sh
docker compose -f $CUMULUS_REPO_PATH/compose.yaml \
  run --volume $CUMULUS_REPO_PATH:/cumulus-etl --rm \
  cumulus-etl \
  --s3-region=us-east-2 \
  /cumulus-etl/tests/data/simple/input \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir1/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir1/
```

You should now be able to see some (very small) output files in your S3 buckets!

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
Follow the [S3 setup guide](set-up-aws.md) document for guidance there.

### Strongly Recommended Arguments

While technically optional, it's recommended that you manually specify these arguments because their
defaults are subject to change or might not match your situation.

* `--input-format`: There are two reasonable values (`ndjson` and `i2b2`). If you want to pull from
  your bulk export FHIR server, pass in its URL as your input path and use `ndjson` as your input
  format. Otherwise, you can use either value to point at a local folder with either FHIR ndjson
  or i2b2 csv files sitting in them, respectively.

* `--output-format`: There are two reasonable values (`ndjson` and `deltalake`).
  For production use, you want `deltalake` as it is supports incremental, batched updates.
  But `ndjson` is useful when debugging as it is human-readable.

* `--batch-size`: How many resources to save in a single output file. If there are more resources
  (e.g. more patients) than this limit, multiple output files will be created for that resource
  type. The larger the better for performance reasons, but that will also take more memory.
  So this number is highly environment specific. For 16GB of memory, we recommend `300000`.

### Optional Arguments

* `--comment`: You can add any comment you like here, it will be saved in a logging folder on the
output path (`JobConfig/`). Might help when debugging an issue.

### Performance

For tips and tricks around what hardware to use or how to run Cumulus on a GPU for faster natural language processing,
see the separate [performance how-to](etl-performance.md).

### Bulk FHIR Export

A common task is to point Cumulus ETL at your bulk FHIR export server, rather than pointing it
at a local folder with the exported data.

This is relatively straightforward but requires a bit of setup first.

#### Registering Cumulus ETL

On your server, you need to register a new "backend service" client.
You'll be asked to provide a JWKS (JWK Set) file.
See below for generating that.
You'll also be asked for a client ID or the server may generate a client ID for you.

#### Generating a JWKS

A JWKS is just a file with some cryptographic keys,
usually holding a public and private version of the same key.
FHIR servers use it to grant clients access.

You can generate a JWKS using the RS384 algorithm and a random ID by running the command below.

(Make sure you have `jose` installed first.)

```sh
jose jwk gen -s -i "{\"alg\":\"RS384\",\"kid\":\"`uuidgen`\"}" -o rsa.jwks
```

Then give `rsa.jwks` to your FHIR server and to Cumulus ETL (details on that below).

#### Cumulus ETL Arguments

You'll need to pass two new arguments to Cumulus ETL: 

```sh
--smart-client-id=YOUR_CLIENT_ID
--smart-jwks=/path/to/rsa.jwks
```

You can also give `--smart-client-id` a path to a file with your client ID,
if it is too large and unwieldy for the commandline.

And for Cumulus ETL's input path argument,
you will give your server's URL address,
including a Group identifier if you want to scope the export
(e.g. `https://example.com/fhir` or `https://example.com/fhir/Group/1234`).

#### Narrowing Export Scope

You can pass `--since=` and/or `--until=` to narrow your bulk export to a date range.

Note that support for these parameters among EHRs is not super common.
- `--since=` is in the FHIR spec but is not required by law.
  (And notably, it's not supported by Epic.)
- `--until=` is not even in the FHIR spec yet. No major EHR supports it.

But if you are lucky enough to be working with an EHR that supports either one,
you can pass in a time like `--since=2023-01-16T20:32:48Z`.

#### Saving Bulk Export Files

Bulk exports can be tricky to get right and can take a long time.
Sometimes (and especially when first experimenting with Cumulus ETL),
you will want to save the results of a bulk export for inspection or in case Cumulus ETL fails.

By default, Cumulus ETL throws away the results of a bulk export once it's done with them.
But you can pass `--export-to=/path/to/folder` to instead save the exported `.ndjson` files in the given folder.

Note that you'll want to expose the local path to docker so that the files reach your actual disk, like so:

```sh
docker compose \
  run --rm \
  --volume /my/exported/files:/folder \
  cumulus-etl \
  --export-to=/folder \
  https://my-fhir-server/ \
  s3://output/ \
  s3://phi/
```

#### External Bulk Export

Instead of using Cumulus ETL to drive your bulk export itself, you can instead do the bulk export externally,
and then just feed the resulting files to Cumulus ETL.

The [SMART Bulk Data Client](https://github.com/smart-on-fhir/bulk-data-client) is a great tool with more
options than Cumulus ETL's built-in exporter offers.

If you use this tool, pass Cumulus ETL the folder that holds the downloaded data as the input path.
And you may need to specify `--fhir-url=` so that external document notes can be downloaded.

## EHR-Specific Advice

Different EHRs have different features and performance.
Here is some EHR-specific documentation:

- [Cerner](cerner-tips.md)
- [Epic](epic-tips.md)

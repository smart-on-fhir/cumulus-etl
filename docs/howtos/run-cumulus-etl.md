<!-- Target audience: engineer familiar with the project, helpful direct tone -->

# How To Run Cumulus ETL at Your Hospital

This guide will explain how to install and run Cumulus ETL inside your hospital's infrastructure.
It assumes you are familiar with the command line.

## Setup

Before we dive in, you're going to want the following things in place:

1. A server on which to run Cumulus ETL, with access to your bulk patient data
    * This could be a long-running server or a disposable per-run instance.
      Whatever works for your hospital setup.
2. A [UMLS](https://www.nlm.nih.gov/research/umls/index.html) API key
    * Your hospital probably already has one, but they are also easy to
      [request](https://www.nlm.nih.gov/databases/umls.html).
3. A deploy key for the Cumulus ETL repository (see below)

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
yum -y install docker-ce docker-ce-cli containerd.io
systemctl start docker
systemctl enable docker.service
systemctl enable containerd.service
```

## Docker Compose

[Docker Compose](https://docs.docker.com/compose/) is included with Docker, and allows you to deploy a self-contained network.
We're using it to simplify deploying the Cumulus ETL project in your ecosystem.

The docker-compose.yml, which defines the network, adds the following containers:

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
CUMULUS_REPO_PATH =/path-to-cloned-cumulus-etl-repo
COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker compose -f $CUMULUS_REPO_PATH/docker-compose.yml build
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
to start the network:
```sh
UMLS_API_KEY=your-umls-api-key
docker compose -f $CUMULUS_REPO_PATH/docker-compose.yml up -d
```

The docker-compose file will handle the environment variable mapping and volume mounts for you.
After running that command, you can start the actual etl process with the following command:
```sh
docker compose -f $CUMULUS_REPO_PATH/docker-compose.yml run cumulus-etl \
  /cumulus-etl/tests/data/simple/ndjson-input \
  /cumulus-etl/example-output \
  /cumulus-etl/example-phi-build \
  --output-format=ndjson
```

After running this command, you should be able to see output in
`$CUMULUS_REPO_PATH/example-output` and some build artifacts in
`$CUMULUS_REPO_PATH/example-phi-build`.

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
docker compose -f $CUMULUS_REPO_PATH/docker-compose.yml run cumulus-etl \
  --s3-region=us-east-2 \
  /cumulus-etl/tests/data/simple/ndjson-input \
  s3://my-cumulus-prefix-99999999999-us-east-2/subdir1/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir1/
```

You should now be able to see some (very small) output files in your S3 buckets!

### More Realistic Example Command

Here's a more realistic and complete command, as a starting point for your own version.

```sh
docker compose -f $CUMULUS_REPO_PATH/docker-compose.yml run \
 cumulus-etl \
  --comment="Any interesting logging data you like, like which user launched this" \
  --input-format=ndjson \
  --output-format=parquet \
  --batch-size=10000000 \
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

* `--output-format`: There are two reasonable values (`ndjson` and `parquet`). For production use,
  you probably want `parquet` as it is smaller and faster. But `ndjson` is useful when debugging as
  it is human-readable.

* `--batch-size`: How many resources to save in a single output file. If there are more resources
  (e.g. more patients) than this limit, multiple output files will be created for that resource
  type. The larger the better for performance reasons, but that will also take more memory.
  So this number is highly environment specific.

### Optional Arguments

* `--comment`: You can add any comment you like here, it will be saved in a logging folder on the
output path (`JobConfig/`). Might help when debugging an issue.

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

You can generate a JWKS using the RS384 algorithm and a random ID yourself like so:

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
you will give your server's URL address (e.g. `https://example.com/fhir`).

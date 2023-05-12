---
title: Performance Tuning
parent: ETL
nav_order: 10
# audience: engineer familiar with the project
# type: howto
---

# How To Improve Cumulus ETL Performance

This guide will explain how to optimize your runs of Cumulus ETL.
Time is money, especially when you might be paying for cloud compute time.

It will also talk a bit about the minimum hardware requirements you'll want.

## Memory Consumption

Cumulus ETL can be tuned to use more or less memory.
It can squeeze by with very little, if you tell it to use less memory.

The trade-off to using less memory is that the output files will be smaller,
because Cumulus ETL will hold smaller batches in memory.
This in turn means that the Athena queries against them may take more time.
Which means they cost more.

### Batch Size

The primary way to control memory use is by controlling the batch size.
This is how many rows of data Cumulus ETL will hold in memory at once.

So the larger it is, the more memory will be used and the larger the output files may be.

### Recommended Setup

Use 16GB of memory with `--batch-size=300000`.
We've never found it to run out of memory that way, and it should still have some leeway for spikes of memory use.

If you are using an AWS EC2 instance, we recommend an `m5.xlarge` instance, which has 16GB.

If you have access to more memory, experiment with larger batch sizes and let us know what works.

If you only have access to less memory, try `--batch-size=100000` with 8GB.

## Disk Consumption

We recommend having about four times the size of your input data.
Cumulus ETL makes a copy or two of the transformed data as it goes through the pipeline.

Let's say you have a folder of ndjson FHIR files from a bulk export that is altogether 50GB.
You'd want at least 200GB free.

## Running NLP on a GPU

Most parts of Cumulus ETL are CPU-constrained.
That is, they are mostly concerned with processing data as fast as possible and a faster CPU would help with that.

But the natural language processing (NLP) that Cumulus ETL does can also be _hugely_ sped up with access to a GPU.
An order of magnitude faster.

For example, BCH ran NLP on 370k physician notes with just a CPU in just under 6 weeks.
But on a GPU, it took three days.

Note that the machine-learning libraries we use depend on the CUDA framework,
so they will only run on [CUDA enabled NVIDIA cards](https://developer.nvidia.com/cuda-gpus).

### Driver Setup

You'll need two pieces of driver code set up:
- NVIDIA GPU drivers. See their
[installation documentation](https://docs.nvidia.com/datacenter/tesla/tesla-installation-notes/)
for different OS versions.
- NVIDIA Container Toolkit, which provides Docker support. Again, they have
[installation documentation](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html)
for different OS versions.

### On-Premises Access to a GPU

If you are running Cumulus ETL on-premises with access to a local machine running a GPU,
you might as well run all of Cumulus ETL with a GPU.

(In contrast to a hospital that is running Cumulus ETL in the cloud where GPU-hours cost a premium.
If that that describes you, there are further instructions below.)

Cumulus ETL is deployed with Docker images.
And because of the way Docker interacts with the GPU, we define a whole second set of profiles for GPU usage.

Normally, you specify profile & image names in three places:
1. When building the ETL tool:<br>
`docker compose --profile etl build`
2. When starting the support tools (cTAKES etc):<br>
`docker compose --profile etl-support up`
3. When running the ETL tool:<br>
`docker compose run cumulus-etl`

To work with the GPU version of Cumulus ETL, just add `-gpu` to each of those names
wherever they appear in [instructions](sample-runs.md):
1. <code>docker compose --profile etl<b>-gpu</b> build</code>
2. <code>docker compose --profile etl-support<b>-gpu</b> up</code>
3. <code>docker compose run cumulus-etl<b>-gpu</b></code>

### Cloud Access to a GPU

Because GPU compute time costs significantly more money than a CPU compute time,
you may want to set up two different runs of Cumulus ETL for any given input data:
- Run all the NLP tasks on a GPU instance
- Run everything else on a CPU instance

See the above on-premises instructions for how to switch between running the CPU vs the GPU profiles of Cumulus ETL.

For each run, you'll want to only select the ETL tasks that are suitable for the machine you are on,
so that you don't duplicate effort.
To do that, pass `--task-filter=cpu` or `--task-filter=gpu` options to Cumulus ETL.

If you are running in AWS, we recommend the `g4dn.xlarge` instance type for GPU access.

#### CPU-Only Example

<pre>
docker compose \
 run --rm \
 cumulus-etl \
 <b>--task-filter=cpu</b> \
 s3://input s3://output s3://phi
</pre>

#### GPU-Only Example

Notice both the docker name change (`-gpu`) and the different filter option.

<pre>
docker compose \
 run --rm \
 cumulus-etl<b>-gpu</b> \
 <b>--task-filter=gpu</b> \
 s3://input s3://output s3://phi
</pre>

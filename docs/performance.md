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

The trade-off to using less memory is that the process will take longer,
because Cumulus ETL will hold smaller batches in memory.
The cost of writing a batch to a Delta Lake is roughly the same regardless of size,
so the more you can squeeze in at once, the better.

### Batch Size

The primary way to control memory use is by controlling the batch size.
This is how many rows of data Cumulus ETL will hold in memory at once.

So the larger it is, the more memory will be used and the larger the output files may be.

### Recommended Setup

We've found `--batch-size=100000` works well for 16GB of memory.
And `--batch-size=1000000` works well for 32GB of memory.
(You might have expected the number to simply double, but there is some static overhead.)

Mileage may vary though, depending on how your FHIR data is formatted.
And some resources are naturally smaller than others.
While the numbers above are a good rule of thumb,
experiment to find what works for your environment.

(Running out of memory will look like a sudden closing of the app.
Docker will just immediately shut the container down when it runs out.)

If you are using an AWS EC2 instance, we recommend an `r8g.xlarge` (four cores, 32GB)
and using a 1M batch size.

## Disk Consumption

We recommend having about four times the size of your input data.
Cumulus ETL makes a copy or two of the transformed data as it goes through the pipeline.

Let's say you have a folder of NDJSON FHIR files from a bulk export that is altogether 50GB.
You'd want at least 200GB free.

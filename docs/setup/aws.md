---
title: AWS
parent: Production Setup
grand_parent: ETL
nav_order: 1
# audience: engineer familiar with the project
# type: howto
---

# How To Set Up AWS for Cumulus

This guide will explain how to configure your AWS cloud to add the S3 buckets, Athena tables,
and more that Cumulus will need. It assumes you are familiar with AWS.

At the end, you'll be given a CloudFormation template that will do all the hard work for you.

## End Goal

By the end of this guide, you'll have multiple S3 buckets to both receive the output of Cumulus ETL
and to store the results of Athena queries.

You'll also have defined a Glue database and crawler to map the output of Cumulus ETL onto Glue
tables, which Athena can then query.

But all of that can be grouped into three different stages:
1. Cumulus ETL (S3 buckets)
2. Glue (tables)
3. Athena (bucket & configuration)

### Cumulus ETL

You're going to create two buckets. One for the de-identified Cumulus ETL output and one for build
artifacts (which holds PHI).
They'll each need similar security policies, but Glue will only look at the output bucket.

You also don't _need_ to store your build artifacts in this S3 bucket.
Once you get to actually running Cumulus ETL, there will be an on-premises option for that.
Though you do need to use an S3 bucket for the de-identified Cumulus ETL output.

These buckets will require encryption and allow access to the user role that is running Cumulus ETL.

### Glue

Glue is an AWS product that creates table schemas based on the files in the Cumulus ETL output
bucket.

You're going to create a database to hold the tables and a crawler that scans the Cumulus ETL
output bucket, creating the tables.

After the first crawler run that creates the tables & schemas,
you'll only need to run it again if new tables get added or schemas change
(maybe your institution started adding new metadata to FHIR resources).

We'll set this crawler to run once a month, to pick up the occasional schema changes.
But it can also be run manually when you know a change occurred, or a new table was added.

### Athena

Athena is an AWS product that can run SQL queries against Glue tables.
It's how we'll generate the patient counts for studies.

You're going to create a bucket to hold Athena query results and an Athena workgroup to configure
Athena.

## Cloud Formation

The easy way to set this all up is simply use a CloudFormation template.
[Here's an example template](cumulus-aws-template.yaml) that should work for your needs,
but can be customized as you like.

That's worth emphasizing - your setup does not need to be this exact CloudFormation setup.
This is provided merely for convenience, but if you want to use one bucket instead of three
or change the name of the database, that's totally fine.
The important thing is that you end up with a crawler that feeds data to a database for Athena.

Though if you do use this template, note that it takes five parameters:
1. Bucket prefix
1. Database prefix
1. ETL Subdirectory, matching the subdirectory you pass to Cumulus ETL
1. KMS key ARN for encryption
1. Upload Role ARN, matching the user that runs Cumulus ETL

Once you create this CloudFormation stack, that's it!
You've configured AWS to receive data from Cumulus ETL.
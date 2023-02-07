<!-- Target audience: engineer familiar with the project, helpful direct tone -->

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
(maybe your hospital started adding new metadata to FHIR resources).

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

It takes four parameters:
1. Bucket prefix
1. ETL Subdirectory, matching the subdirectory you pass to Cumulus ETL
1. KMS key ARN for encryption
1. Upload Role ARN, matching the user that runs Cumulus ETL

Once you create this CloudFormation stack, you're almost done.
Only one step left below: updating the Glue crawler.

## Delta Lake Crawler Support

Cumulus uses Delta Lakes to store your data.
AWS Glue support for them is fairly new (September 2022),
and CloudFormation does not yet support that syntax.

But that's easy enough to work around.
We'll just manually update the crawler to point at our delta lakes.

Run the command below to update the crawler you just defined above,
and replace `REPLACE_ME` with a bucket path (the bucket name and `EtlSubdir` you used above).

For example, you might use `s3://my-cumulus-prefix-99999999999-us-east-2/subdir1`.

(Make sure you have `jq` installed first.)

```sh
aws glue update-crawler --name cumulus --targets "`jq -n --arg prefix REPLACE_ME '{"DeltaTargets": [{"DeltaTables": [$prefix+"/condition", $prefix+"/covid_symptom__nlp_results", $prefix+"/documentreference", $prefix+"/encounter", $prefix+"/observation", $prefix+"/patient"], "WriteManifest": false}]}'`"
```

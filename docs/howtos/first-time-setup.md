<!-- Target audience: engineer familiar with the project, upbeat direct tone -->

# How To Set Up Cumulus for the First Time

Welcome to Cumulus!
Let's get you set up.

This guide assumes you have administrative access to your AWS environment.

## AWS Infrastructure

Speaking of, we'll start with creating the AWS buckets and configuration that Cumulus will need.

Follow the separate [AWS setup guide](set-up-aws.md) and come back here when you're done.

## Cumulus ETL Test Run

Now that there are AWS buckets ready to receive Cumulus ETL output, let's do a sample run with
test data.

Follow the separate [Cumulus ETL setup guide](run-cumulus-etl.md) and come back here after
doing the suggested AWS test run of Cumulus ETl.

You should now have some files in your output buckets, but we can't use them yet.

## Create Tables with Glue

Part of the AWS setup was creating a Glue crawler.
This is a process you can kick off that will scan your output bucket and generate table schemas
based on the data it finds.

So now that there is data to scan, let's scan.
This will create the tables that Athena will query against.

1. Go to the `AWS Glue` product in your AWS console.
1. Click on the `Crawlers` menu entry on the left (you may need to first expand the sidebar).
1. You should see a `cumulus` crawler in your list of crawlers. Click into it.
   (If you don't see it there, make sure that you are in the correct AWS region.)
1. Click the `Run crawler` button.

The crawler will start, think for a bit, and then finish.
You can see its status at the bottom of the page.
(Click the small refresh button next to `Stop run` to update the status report.)

When it finishes, it should say something like `6 table changes, 0 partition changes`.

If you now click on the `Tables` menu entry on the left, you can see the tables it created.

This crawler step needs to be re-run when Cumulus ETL adds or changes columns in its
output format.
You'll want to re-run it again once you do your first _real_ Cumulus ETL run, because your full
data set will introduce more columns than the toy test data we used here has.

## Athena Queries

Athena is a way to use SQL queries against the Glue tables.
Let's do a test query to confirm it works.

1. Go to the `Athena` product in your AWS console.
1. Click into the `Query editor` (from the sidebar or a button on the homepage).
1. On the right, select `cumulus` from the `Workgroup` dropdown.
1. On the left, select `cumulus` from the `Database` dropdown.
1. You should see a list of tables on the bottom left, matching the tables in AWS Glue.
1. Enter `select * from cumulus.patient;` in the query editor box and click `Run`.

You should see some results (two patients) in the Results box at the bottom of the page.

Congratulations!
You've set up all the AWS permissions and configuration you need to run Cumulus ETL and query its
results.

## Going Forward

Now that you've done a end-to-end run (well, a FHIR-to-Athena run),
the next step is to finish the Cumulus ETL guide and customize your runner for your environment.

Then once you do your first large-scale Cumulus ETL run, re-run the Crawler (the last time you'll
need to re-run it unless Cumulus changes its format) and Athena will then be ready to query against
your production data.

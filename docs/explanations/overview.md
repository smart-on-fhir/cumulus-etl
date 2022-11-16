<!-- Target audience: non-programmers entirely new to Cumulus, casual tone -->

# What is Cumulus?

The Cumulus project is a dashboard for public health surveillance.

This dashboard lets you monitor symptoms or other health indicators over time,
in the population you are studying.

But it's not _just_ a dashboard.
It's an entire process that shepherds patient data from hospitals into that dashboard,
safely and while preserving privacy.

## OK, but in Concrete Terms, What Does It Do?

Cumulus provides four main features:

1. Natural language processing of physician notes (to extract symptoms, etc)
2. De-identification of patient data
3. Aggregation of multiple hospitals' data into one data stream
4. A dashboard for monitoring health in that aggregate population

# The Cumulus Pipeline

**Cumulus** refers to the entire project pipeline, start to finish.
That is, from hospital servers all the way to pretty graphs in your web browser.
Where it is helpful to disambiguate, the phrase **Cumulus pipeline** might more clearly refer to
the entire process.

There are three main conceptual phases to Cumulus, each holding less and less protected health
information (PHI):

1. Hospital-side data extraction (full PHI)
1. Cloud-side data queries (de-identified)
1. Dashboard monitoring (just counts of patients)

## Hospital-side Data Extraction

Each hospital installs and runs [**Cumulus ETL**](https://github.com/smart-on-fhir/cumulus-etl) on
their own servers at regular intervals.

ETL stands for "extract, transform, load."
Cumulus ETL first **extracts** data from the hospital servers (usually in the form of
a bulk FHIR export).
Then it **transforms** that data by de-identifying it and converting physician notes into lists of
symptoms.
And finally it **loads** that data onto the cloud to be consumed by the next phase of the Cumulus
pipeline.

> This phase starts with full PHI data sitting on hospital servers and
> ends with de-identified data sitting on the cloud, ready to be processed.

## Cloud-side Data Queries

Now that all that patient data has been de-identified and is sitting nicely in the hospital's
cloud, it is ready to be inspected and queried.

There are many queries one could perform on all this data, it just depends on what studies are
being run and what symptoms are being monitored.
But each study's queries have the same basic idea: they only provide counts of affected people.

That is worth emphasizing: the end result is simply a number.
No PHI to worry about.

These patient counts are then exported to a centralized Cumulus server that aggregates them across
the various hospitals and studies, making them ready to be used in the next phase of the Cumulus
pipeline.

> This phase starts inside the hospital's own cloud with extensive but de-identified patient data,
> and ends with just patient counts, on a centralized Cumulus server.

## Dashboard Monitoring

Now that we have aggregate patient counts for each study, they are ready to be presented to actual
humans in a useful way.

The [**Cumulus Dashboard**](https://github.com/smart-on-fhir/cumulus-app) is a web app that shows
each study's results with pretty charts and graphs.
It updates over time as new data comes in from hospitals,
lets you filter down to just the results you are interested in,
and has other useful features for monitoring a study's population.

> This phase starts at a centralized Cumulus server and ends with a delighted researcher.

---
title: ETL
has_children: true
# audience: non-programmers new to Cumulus
# type: explanation
---

# Cumulus ETL

**Cumulus ETL is your entry point into the whole Cumulus pipeline.**

ETL stands for "extract, transform, load."

1. Cumulus ETL first **extracts** data from EHR servers
   (usually in the form of a [bulk FHIR export](bulk-exports.md)).
1. Then it **transforms** that data by [de-identifying](deid.md) it and converting clinical notes
   into lists of symptoms.
1. And finally it **loads** that data onto the cloud to be consumed by the
   [next phase](https://docs.smarthealthit.org/cumulus/library/) of the Cumulus pipeline.

## Installing

Read the [Local Test Setup](local-setup.md) documentation to learn how to install & run
Cumulus ETL on your machine with sample data, as an introduction to the flow.

Then you can move on to the [Production Setup](setup) instructions to set up the AWS infrastructure
that the full Cumulus pipeline will require.

## Source Code
Cumulus ETL is open source.
If you'd like to browse its code or contribute changes yourself,
the code is on [GitHub](https://github.com/smart-on-fhir/cumulus-etl).
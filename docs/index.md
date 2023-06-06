---
title: ETL
has_children: true
# audience: non-programmers new to Cumulus
# type: explanation
---

# Cumulus ETL

**Cumulus ETL is your entry point into the whole Cumulus pipeline.**

ETL stands for "extract, transform, load."

1. Cumulus ETL first **extracts** data from the hospital servers
   (usually in the form of a [bulk FHIR export](bulk-exports.md)).
1. Then it **transforms** that data by [de-identifying](deid.md) it and converting clinical notes
   into lists of symptoms.
1. And finally it **loads** that data onto the cloud to be consumed by the
   [next phase](https://docs.smarthealthit.org/cumulus/library/) of the Cumulus pipeline.

## Installing

Read the [First Time Setup](setup) documentation to learn how to install & prepare Cumulus ETL
at your institution.
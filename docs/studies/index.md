---
title: Studies
parent: ETL
nav_order: 3
has_children: true
# audience: engineer familiar with the project
# type: howto
---

# Study-specific Tasks

In addition to the default basic-FHIR-oriented Cumulus ETL tasks like `condition`,
which simply strip identifying information and largely leaves the FHIR alone,
there are also more interesting study-oriented tasks.

These tend to be [NLP](../nlp.md) tasks that extract information from clinical notes.

They aren't run by default,
but you can provide the ones you are interested in with the `--task` parameter.

In this folder, you can read further explanations of how to run each built-in study.

---
title: Supported Models
parent: NLP
grand_parent: ETL
nav_order: 2
# audience: engineer familiar with the project
# type: reference
---

# Supported NLP Models

There are multiple ways to access the NLP models a study will need.

For example, a hypothetical `study__nlp_gpt_oss_120b` task that uses the GPT-OSS 120b model,
can use Azure, Bedrock, or a local on-prem version of the model.

Below, you can see a support matrix of the models and platforms that Cumulus ETL supports.
Individual study support might be more limited, depending on the study design.
But these are all the models that a study _could_ make use of:

|                   | Azure | Bedrock | Local |
|-------------------|-------|---------|-------|
| Claude Sonnet 4.5 | ❌     | ✅       | ❌     |
| GPT 3.5           | ✅     | ❌       | ❌     |
| GPT 4             | ✅     | ❌       | ❌     |
| GPT 4o            | ✅     | ❌       | ❌     |
| GPT 5             | ✅     | ❌       | ❌     |
| GPT OSS           | ✅     | ✅       | ✅     |
| Llama4 Scout      | ✅     | ✅       | ✅     |

## Using Azure

- Pass `--provider=azure` when running NLP (e.g. `cumulus-etl nlp --provider=azure ...`)
- Set `AZURE_OPENAI_API_KEY` and `AZURE_OPENAI_ENDPOINT` environment variables

## Using Bedrock

- Pass `--provider=bedrock` when running NLP (e.g. `cumulus-etl nlp --provider=bedrock ...`)
- Set the appropriate AWS environment variables, like `AWS_PROFILE` or `AWS_ACCESS_KEY_ID` etc.

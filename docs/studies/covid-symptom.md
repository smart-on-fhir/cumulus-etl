---
title: Covid Symptom
parent: Studies
grand_parent: ETL
nav_order: 1
# audience: engineer familiar with the project
# type: howto
---

# The Covid Symptom Study

This study uses NLP to identify symptoms of COVID-19 in clinical notes.

It allows for running different NLP strategies and comparing them:
1. [cTAKES](https://ctakes.apache.org/) and the `negation`
[cNLP transformer](https://github.com/Machine-Learning-for-Medical-Language/cnlp_transformers)
2. cTAKES and the `termexists` cNLP transformer
3. [ChatGPT](https://openai.com/chatgpt/) 3.5
4. ChatGPT 4

Each can be run separately, and may require different preparation.
Read more below about the main approaches (cTAKES and ChatGPT).

## cTAKES Preparation

First, you'll want to [register](https://www.nlm.nih.gov/databases/umls.html)
for a [UMLS](https://www.nlm.nih.gov/research/umls/index.html) API key.

Then because cTAKES and cNLP transformers are both services separate from the ETL,
you will want to make sure they are ready.

From your working directory with the Cumulus ETL's `compose.yaml`,
you can run the following to start those services:
```shell
export UMLS_API_KEY=your-umls-api-key  # don't forget to set this - cTAKES needs it
docker compose --profile covid-symptom-gpu up --wait
```

You'll notice the `-gpu` suffix there.
Running the transformers is much, much faster with access to a GPU,
so we strongly recommend you run this on GPU-enabled hardware.

And since we _are_ running the GPU profile, when you do run the ETL,
you'll want to launch the GPU mode instead of the default `cumulus-etl` CPU mode:
```shell
docker compose run cumulus-etl-gpu …
```

But if you can't use a GPU or you just want to test things out,
you can use `--profile covid-symptom` above and the normal `cumulus-etl` run line to use the CPU.

## ChatGPT Preparation

1. Make sure you have an Azure ChatGPT account set up.
2. Set the following environment variables:
   - `AZURE_OPENAI_API_KEY`
   - `AZURE_OPENAI_ENDPOINT`

## Running the Tasks

To run any of these individual tasks, use the following names:

- cTAKES + negation: `covid_symptom__nlp_results`
- cTAKES + termexists: `covid_symptom__nlp_results_term_exists`
- ChatGPT 3.5: `covid_symptom__nlp_results_gpt35`
- ChatGPT 4: `covid_symptom__nlp_results_gpt4`

For example, your Cumulus ETL command might look like:
```sh
cumulus-etl … --task=covid_symptom__nlp_results
```

### Clinical Notes

All these tasks will need access to clinical notes,
which are pulled fresh from your EHR (unless you [inlined](../bulk-exports.md) your notes).
This means you will likely have to provide some other FHIR authentication arguments like
`--smart-client-id` and `--fhir-url`.

See `--help` for more authentication options.

## Evaluating the Results

See the [Cumulus Library Covid study repository](https://github.com/smart-on-fhir/cumulus-library-covid)
for more information about processing the raw NLP results that the ETL generates.

Those instructions will help you set up Label Studio so that you can compare the
different NLP strategies against human reviewers.

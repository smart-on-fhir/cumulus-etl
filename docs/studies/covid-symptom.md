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
Specifically, it uses [cTAKES](https://ctakes.apache.org/) and
[cNLP transformers](https://github.com/Machine-Learning-for-Medical-Language/cnlp_transformers)
to identify clinical terms.

## Preparation

Because it uses external services like cTAKES, you will want to make sure those are ready.
From your git clone of the `cumulus-etl` repo, you can run the following to run those services:
```shell
export UMLS_API_KEY=your-umls-api-key  # don't forget to set this - cTAKES needs it
docker compose --profile covid-symptom-gpu up -d
```

You'll notice the `-gpu` suffix there.
Running NLP is much, much faster with access to a GPU,
so we strongly recommend you run this on GPU-enabled hardware.

And since we _are_ running the GPU profile, when you do run the ETL,
you'll want to launch the GPU mode instead of the default `cumulus-etl` CPU mode:
```shell
docker compose run cumulus-etl-gpu …
```

But if you can't use a GPU or you just want to test things out,
you can use `--profile covid-symptom` above and the normal `cumulus-etl` run line to use the CPU.

## Task

There is one main task, run with `--task covid_symptom__nlp_results`.

This will need access to clinical notes,
which are pulled fresh from your EHR (since the ETL doesn't store clinical notes).
This means you will likely have to provide some other FHIR authentication arguments like
`--smart-client-id` and `--fhir-url`.

See `--help` for more authentication options.

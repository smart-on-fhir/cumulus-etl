---
title: Example Workflow
parent: NLP
grand_parent: ETL
nav_order: 1
# audience: engineer familiar with the project
# type: tutorial
---

# An Example NLP Workflow

Let's work through an end-to-end NLP workflow, as if you were doing a real study.
But we'll use an example study shipped with the ETL for testing purposes instead.

This will take us from the initial actual NLP, then to chart review,
then finally to analyzing accuracy.

You don't need to prepare your own clinical notes for this run-through.
We'll use synthetic notes shipped with Cumulus ETL for this very purpose.

This example study we'll use is just a very simple age range study.
The NLP will only be tasked with extracting an age from a clinical note.

## The NLP Itself

Before we start, you'll need to have Cumulus ETL and your AWS infrastructure ready.
Follow the [setup instructions](../setup) if you haven't done so already, then come back here.

### Model Setup

You have a choice of model for this.
Real studies might require one specific model or another.
But this example task is fairly liberal.

Here are the options, along with the task name to use.

#### Azure Cloud Options
For these, you'll want to set a couple variables first:
```sh
export AZURE_OPENAI_API_KEY=xxx
export AZURE_OPENAI_ENDPOINT=https://xxx.openai.azure.com/
```

Task names:
- GPT4: `example__nlp_gpt4`
- GPT4o: `example__nlp_gpt4o`
- GPT5: `example__nlp_gpt5`

This should cost you less than 15 cents to run and could be much less depending on the model.
We'll use less than five thousand tokens.

#### Local (On-Prem) Options
For these, you'll need to start up the appropriate model on your machine:
```sh
docker compose up --wait gpt-oss-120b
```

Task names:
- GPT-OSS 120B (needs 80GB of GPU memory): `example__nlp_gpt_oss_120b`

### Running the ETL

Now that your model is ready, let's run the ETL on some notes!

Below is the command line to use.
You'll need to change the bucket names and paths to wherever you set up your AWS infrastructure.
And you'll want to change the task name as appropriate for your model.
Leave the odd looking `%EXAMPLE%` bit in place;
that just tells Cumulus ETL to use its built-in example documents as the input.

The output and PHI bucket locations should be the same as your normal ETL runs on raw FHIR data.
There's no actual PHI in this example run because of the synthetic data,
but normally there is, and that PHI bucket is where Cumulus ETL keeps caches of NLP results.

```sh
docker compose run --rm \
  cumulus-etl nlp \
  %EXAMPLE% \
  s3://my-output-bucket/ \
  s3://my-phi-bucket/ \
  --task example__nlp_gpt4
```

(If this were a real study, you'd probably do this a bit differently.
You'd point at your real DocumentReference resources for example.
And you'd probably restrict the set of documents you run NLP on with an argument
like `--cohort-athena-table study__my_cohort`.
But for this run-through, we're going to hand-wave all the document selection pieces.)

### Running the Crawler

Whenever you write a new table to S3, you'll want to run your AWS Glue crawler again,
so that the table's schema gets set correctly in Athena.

First, confirm that your AWS Cloud Formation templates have the `example__nlp_*` tables
configured in them. If not, try copying the Glue crawler definition from
[the sample template we provide](../setup/aws.md).

Then go to your AWS console, in the AWS Glue service, in the sidebar under Data Catalog, and
choose Crawlers.
You should see your crawler listed there. Select it, click Run, and wait for it to finish.

### Confirm the Data in Athena

While you're in the AWS console, switch to the Athena service and select the appropriate
Cumulus workgroup and database.

Then if you make a query like below (assuming you used the GPT4 model),
you should see eight results with extracted ages.
```sql
select * from example__nlp_gpt4
```

**Congratulations!**
You've now run NLP on some synthetic clinical notes and uploaded the results to Athena.
Those extracted ages could now be post-processed by the `example` study to calculate age ranges,
and then confirmed with chart review by humans.

At least that's the flow you'd use for a real study.

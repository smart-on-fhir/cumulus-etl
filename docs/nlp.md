---
title: NLP
parent: ETL
nav_order: 5
# audience: non-programmers, conversational tone, selling a bit
# type: explanation
---

# How Does Cumulus ETL Use NLP?

One of the big features of Cumulus is the ability to easily run natural language processing (NLP)
over your clinical notes.

The general idea is that Cumulus ETL runs NLP over your clinical notes,
extracts symptoms of interest,
and records those findings alongside the coded FHIR data.

This way you can often surface symptoms that simply aren't recorded in the traditional FHIR data.

## NLP Is Always Specific to a Study

The first thing to understand is that Cumulus ETL always
runs NLP in the context of a specific study purpose.

Each study's design will have its own needs and its own NLP strategy,
so we support multiple approaches.

{: .note }
Example: The `covid_symptom` study uses cTAKES and a negation transformer working together to tag
COVID-19 symptoms in clinical notes.
But another study might use the Llama2 large language model,
with a prompt like "Does this patient have a nosebleed?"

## Available NLP Strategies

### cTAKES

[Apache cTAKES](https://ctakes.apache.org/) is a tried and true method of tagging symptoms in text.

A Cumulus ETL study can pass clinical notes to cTAKES and augment its results by using:
- a custom dictionary to focus specifically on terms of interest
- a [cNLP transformer](https://github.com/Machine-Learning-for-Medical-Language/cnlp_transformers)
  to improve negation detection (e.g. "does not have a fever" vs "has a fever")

### Llama2

Meta's [Llama2](https://ai.meta.com/llama/) is a freely available large language model (LLM).

A Cumulus ETL study can pass a carefully-crafted prompt and clinical notes to Llama2
and record the answer.
Since Llama2 is run locally, your notes never leave your network.

The answer you receive might need additional post-processing, to get to a simple yes/no.
Just depends on the study & the prompt.

### Others

There are plans to add the ability to talk to cloud LLMs, like ChatGPT.

Or any other new transformers or services could be integrated, as needed.
If a new study required a new service, Cumulus ETL can add support for it.

## Technical Workflow

How does it all really work though?
Be warned that this next section will get a little technical.

### Docker Integration

Services like cTAKES and Llama2 can be launched with a single command,
because we ship Docker definitions for them.

All _you_ have to bring to the table is your own GPU hardware.

#### Example

As an example, let's say you want to run the `covid_symptom` study.
The command below will launch all the services that study needs.
In this case, that means cTAKES and two different cNLP transformers.
```shell
docker compose --profile covid-symptom-gpu up -d
```

That command works because Cumulus ETL ships a Docker Compose file with stanzas like:
```yaml
ctakes-covid:
  image: smartonfhir/ctakes-covid:1.1.0
  environment:
    - ctakes_umlsuser=umls_api_key
    - ctakes_umlspw=$UMLS_API_KEY
  networks:
    - cumulus-etl
  profiles:
    - covid-symptom-gpu
```

Docker will download the referenced image and launch it with the specified configuration.

### Study Task

Once you've prepared the services the study will need with Docker Compose,
you can actually run Cumulus ETL on your clinical notes.

1. Run the specific NLP study task you are interested in.
   For example, `docker compose run etl-gpu --task covid_symptom__nlp_results …`
2. Cumulus ETL will read your DocumentReference FHIR resources.
3. It will download the clinical notes mentioned by those DocumentReferences.
4. It will feed those notes to an NLP service (in this case, to cTAKES).
5. It will write the results (but not the note!) out to an Athena database,
   just like it does with basic FHIR resources.
   In this example, the results might be a list of COVID-19 symptoms that cTAKES found in the note.

#### Where Does the Note/PHI Live?

After Cumulus ETL downloads the clinical note and runs NLP on it,
it no longer needs the note.

The note is never pushed to Athena (only the NLP results are).

Some aspects of the note might be cached.
For example cTAKES results are cached, so that we only need to run cTAKES once per note.
But that will be in a special PHI-capable folder that you provide Cumulus ETL with.
That is separate location from the Athena databases and can be entirely local to your machine.

### NLP Results

The NLP responses are written to an Athena database and can be queried using SQL.
Usually by study-specific SQL integrated into the
[Cumulus Library](https://docs.smarthealthit.org/cumulus/library/).

In the `covid_symptom` example we've been using,
the Athena database row for a `fever` cTAKES match in a clinical note would look something like
(in json form):
```json
{
  "id": "<anonymized ID>",
  "docref_id": "<anonymized ID>",
  "encounter_id": "<anonymized ID>",
  "subject_id": "<anonymized ID>",
  "generated_on": "2020-01-20T20:00:00+00:00",
  "task_version": 3,
  "match": {
    "begin": 36,
    "end": 41,
    "text": "fever",
    "polarity": 0,
    "conceptAttributes": [
      {"code": "386661006", "cui": "C0015967", "codingScheme": "SNOMEDCT_US", "tui": "T184"},
      {"code": "50177009", "cui": "C0015967", "codingScheme": "SNOMEDCT_US", "tui": "T184"}
    ],
    "type": "SignSymptomMention"
  }
}
```

As the Cumulus Library SQL processes all the detected symptoms
and cross-references the patients & encounters,
it generates counts of patients with fever, headaches, etc.
Those counts are then sent to the Cumulus Dashboard and
can then finally be displayed as digestible charts.

And that's the lifecycle of a clinical note!
It starts inside your EHR,
flows through the ETL & related NLP services,
its symptoms end up in Athena,
and counts of those symptoms get sent to & displayed in the Dashboard.

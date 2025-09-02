---
title: Chart Review
parent: ETL
nav_order: 20
# audience: engineer familiar with the project
# type: tutorial
---

# Chart Review

Chart review is a critical part of study validation.

Cumulus ETL offers an upload mode,
where it sends clinical notes to your own [Label Studio](https://labelstud.io/)
instance for expert review.
Along the way, it can mark the note with NLP results and/or anonymize the note with
[philter](https://github.com/SironaMedical/philter-lite).

This is useful for not just actual chart reviews, but also for developing a custom NLP dictionary.
You can feed Cumulus ETL a custom NLP dictionary, review how it performs, and iterate upon it.

## Preliminaries

### Label Studio Setup

This guide assumes you already have a local instance of Label Studio running.
They offer Docker images and reasonable
[installation docs](https://labelstud.io/guide/install.html).
If you haven't set that up yet, go do that and come back.

The Cumulus team can help you with setting it up if you come talk to us,
but the rest of this guide will mostly deal with the `upload-notes` mode itself.

## Basic Operation

At its core, upload mode is just another ETL (extract, transform, load) operation.
1. It extracts DiagnosticReport and/or DocumentReference resources from your EHR.
2. It transforms the contained notes via `philter` (and optionally NLP).
3. It loads the results into Label Studio.

### Minimal Command Line

Upload mode takes three main arguments:
1. Input path (local dir of NDJSON)
2. URL for Label Studio
3. PHI/build path (the same PHI/build path you normally provide to Cumulus ETL)

Additionally, there are two required Label Studio parameters:
1. `--ls-token PATH` (a file holding your Label Studio authentication token)
2. `--ls-project ID` (the number of the Label Studio project you want to push notes to)

Taken altogether, here is an example minimal `upload-notes` command:
```sh
docker compose run --rm \
 --volume /local/path:/host \
 cumulus-etl upload-notes \
  --ls-token /host/label-studio-token.txt \
  --ls-project 3 \
  --s3-region=us-east-2 \
  /host/my-input-folder/ \
  https://my-label-studio-server/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir/
```

The above command will read all the DiagnosticReports and DocumentReferences in the input folder,
anonymize the notes with `philter`,
and then push the results to your Label Studio project number `3`.

### Grouping by Encounter

Upload mode will group all notes by encounter and present them together as a single
Label Studio artifact by default.

Each clinical note will have a little header describing what type of note it is ("Admission MD"),
as well as its real & anonymized resource identifiers,
to make it easier to reference back to your EHR or Athena data.

If grouping by encounter doesn't make sense for your task,
you can turn it off with `--grouping=none`.

## Downloading Notes

Ideally your notes are already downloaded and inlined into your DiagnosticReport or
DocumentReference NDJSON files.

But if they aren't, you can pass all the normal FHIR server authentication options and
the clinical notes will be downloaded on the fly.

## Document Selection Options

By default, upload mode will grab _all documents_ in the target folder.
But usually you will probably want to only select a few documents for testing purposes.
More in the realm of 10-100 specific documents.

You have a few options here, very similar to the options for selecting notes for NLP or
labeling notes during upload:
- Select by .csv file (anonymous or not)
- Select by Athena table
- Select by word search

Use `--help` to see all the options, but we'll explore a couple below in more detail.

### By Note ID
If you happen to know the original (pre-anonymized) IDs for the documents you want, that's easy!

Make a csv file that has a `note_ref` column, with those note IDs.
For example:
```
note_ref
DocumentReference/123
DocumentReference/6972
DiagnosticReport/1D3D
```

Then pass in an argument like `--select-by-csv /host/docrefs.csv`.

Upload mode will only export & process the specified documents, saving a lot of time.

### By Athena Table
If you are working with your existing de-identified limited data set in Athena,
you will only have references to the anonymized document IDs and no direct clinical notes.

But that's fine!
Upload mode can detect which notes match which anonymous IDs because it can use the same
anonymization approach on each source note and see which IDs match the requested set.

Simply pass in an argument like `--select-by-athena-table my_database_name.my_table_name`.
(And `--athena-workgroup my_workgroup`.)
Upload mode will search the table and match its anonymous IDs to the notes in your input folder.

### Saving the Selected Documents

It might be useful to save the smaller selection from a giant input folder,
for faster iterations of the upload mode or
just confirming the correct documents were chosen.

Pass in an argument like `--export-to /host/export` to save the NDJSON for the selected documents
in the given folder. (Note this does not save the clinical note text unless it is already inline
-- this is just saving the DocumentReference resources).

## NLP

To enable NLP tagging via cTAKES, pass `--nlp`.
This will tag mentions of symptoms it finds, to make chart review easier.

### Dependent Services

NLP needs cTAKES to be available, so you'll need to launch it before you begin:

```shell
export UMLS_API_KEY=your-umls-api-key
docker compose up --wait --profile upload-notes
```

Or if you have access to a GPU,
you can speed up the NLP by launching the GPU profile instead with `--profile upload-notes-gpu`.

### Custom Dictionaries

If you would like to customize the dictionary terms that are marked up and sent to Label Studio,
simply pass in a new dictionary like so: `--symptoms-bsv /host/my-symptoms.bsv`.

This file should look like (this is a portion of the default Covid dictionary):
```
##  Columns = CUI|TUI||STR|PREF
##      CUI = Concept Unique Identifier
##      TUI = Type Unique Identifier
##      STR = String text in clinical note (case insensitive)
##     PREF = Preferred output concept label

##  Congestion or runny nose
C0027424|T184|nasal congestion|Congestion or runny nose
C0027424|T184|stuffed-up nose|Congestion or runny nose
C0027424|T184|stuffy nose|Congestion or runny nose
C0027424|T184|congested nose|Congestion or runny nose
C1260880|T184|rhinorrhea|Congestion or runny nose
C1260880|T184|Nasal discharge|Congestion or runny nose
C1260880|T184|discharge from nose|Congestion or runny nose
C1260880|T184|nose dripping|Congestion or runny nose
C1260880|T184|nose running|Congestion or runny nose
C1260880|T184|running nose|Congestion or runny nose
C1260880|T184|runny nose|Congestion or runny nose
C0027424|T184|R09.81|Congestion or runny nose

##  Diarrhea
C0011991|T184|diarrhea|Diarrhea
C0011991|T184|R19.7|Diarrhea
C0011991|T184|Watery stool|Diarrhea
C0011991|T184|Watery stools|Diarrhea
```

Upload mode will only label phrases whose CUI appears in this symptom file.
And the label used will be the last part of each line (the `PREF` part).

That is, with the above symptoms file, the word `headache` would not be labelled at all
because no CUI for headache is listed in the file.
But any phrases that cTAKES tags as CUI `C0011991` will be labelled as `Diarrhea`.

cTAKES has an internal dictionary and knows some phrases already.
But you may get better results by adding extra terms and variations in your symptoms file.

## Philter

You may not need `philter` processing.
Simply pass `--philter=disable` and it will be skipped.

Or alternatively, pass `--philter=label` to highlight rather than redact detected PHI.

## Label Studio

### Overwriting

By default, upload mode will never overwrite any data in Label Studio.
It will push new notes and skip any that were already uploaded to Label Studio.

But obviously, that becomes annoying if you are iterating on a dictionary or
otherwise re-running upload mode.

So to overwrite existing notes, simply pass `--overwrite`.

### Label Config

Before using upload mode, you should have already set up your Label Studio instance.
Read [their docs](https://labelstud.io/guide/) to get started with that.

Those docs can guide you through how to define your labels.
But just briefly, a setup like this with hard-coded labels will work:
```
<View>
  <Labels name="label" toName="text">
    <Label value="Diarrhea" background="#3333cc"/>
    <Label value="Congestion or runny nose" background="#99ccff"/>
  </Labels>
  <Text name="text" value="$text"/>
</View>
```

Or you can use dynamic labels, and upload mode will define them from your symptoms file.
Note that the `value` argument must match the `name` argument in your config, like so:
```
<View>
  <Labels name="label" toName="text" value="$label" />
  <Text name="text" value="$text"/>
</View>
```

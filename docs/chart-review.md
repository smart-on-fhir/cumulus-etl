---
title: Chart Review
parent: ETL
nav_order: 20
# audience: engineer familiar with the project
# type: tutorial
---

# Chart Review

Cumulus ETL also offers a chart review mode,
where it sends clinical notes to [Label Studio](https://labelstud.io/) for expert review.
Along the way, it can mark the note with NLP results and/or anonymize the note with
[philter](https://github.com/SironaMedical/philter-lite).

This is useful for not just actual chart reviews, but also for developing a custom NLP dictionary.
You can feed Cumulus ETL a custom NLP dictionary, review how it performs, and iterate upon it.

## Basic Operation

At its core, chart review mode is just another ETL (extract, transform, load) operation.
1. It extracts DocumentReference resources from your EHR.
2. It transforms the contained notes via NLP & `philter`.
3. It loads the results into Label Studio.

### Minimal Command Line

Chart review mode takes three main arguments:
1. Input path (local dir of ndjson or a FHIR server to perform a bulk export on)
2. URL for Label Studio
3. PHI/build path (the same PHI/build path you normally provide to Cumulus ETL)

Additionally, there are two required Label Studio parameters:
1. `--ls-token PATH` (a file holding your Label Studio authentication token)
2. `--ls-project ID` (the number of the Label Studio project you want to push notes to)

Taken altogether, here is an example minimal chart review command:
```sh
docker compose run \
 --volume /local/path:/in \
 cumulus-etl \
 chart-review \
  --ls-token /in/label-studio-token.txt \
  --ls-project 3 \
  https://my-ehr-server/R4/12345/Group/67890 \
  https://my-label-studio-server/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir1/
```

The above command will take all the DocumentReferences in Group `67890` from the EHR,
mark the notes with the default NLP dictionary,
anonymize the notes with `philter`,
and then push the results to your Label Studio project number `3`.

## Bulk Export Options

You can point chart review mode at either a folder with DocumentReference ndjson files
or your EHR server (in which case it will do a bulk export from the target Group).

Chart review mode takes all the same [bulk export options](bulk-exports.md) that the normal
ETL mode supports.

Note that even if you provide a folder of DocumentReference ndjson resources,
you will still likely need to pass `--fhir-url` and FHIR authentication options,
so that chart review mode can download the referenced clinical notes _inside_ the DocumentReference,
which usually hold an external URL rather than inline note data.

## Document Selection Options

By default, chart review mode will grab _all documents_ in the target Group or folder.
But usually you will probably want to only select a few documents for testing purposes.
More in the realm of 10-100 specific documents.

You have two options here:
selection by either the original EHR IDs or by the anonymized Cumulus IDs.

### By Original ID
If you happen to know the original (pre-anonymized) IDs for the documents you want, that's easy!

Make a csv file that has a `docref_id` column, with those docrefs.
For example:
```
docref_id
123
6972
1D3D
```

Then pass in an argument like `--docrefs /in/docrefs.csv`.

Chart review mode will only export & process the specified documents, saving a lot of time.

### By Anonymized ID
If you are working with your existing de-identified limited data set in Athena,
you will only have references to the anonymized document IDs and no direct clinical notes.

But that's fine!
Chart review mode can use the PHI folder to grab the cached mappings of patient IDs
and then work to reverse-engineer the correct document IDs (to then download from the EHR).

For this to work, you will need to provide both the anonymized docref ID **and**
the anonymized patient ID.

Back in Athena, run a query like this and download the result as a csv file:
```sql
select
  replace(subject_ref, 'Patient/', '') as patient_id,
  doc_id as docref_id
from
  core__documentreference
limit 10;
```

(Obviously, modify as needed to select the documents you care about.)

You'll notice we are defining two columns: patient_id and docref_id (those must be the names).

Then, pass in an argument like `--anon-docrefs /in/docrefs.csv`.
Chart review mode will reverse-engineer the original document IDs and export them from your EHR.

#### I Thought the Anonymized IDs Could Not Be Reversed?

True!
But Cumulus ETL saves a cache of all the IDs it makes for your patients (and encounters).
You can see this cache in your PHI folder, named `codebook-cached-mappings.json`.

By using this mapping file,
chart review mode can find all the original patient IDs using the `patient_id` column you gave it.

Once it has the original patients, it will ask the EHR for all of those patients' documents.
And it will anonymize each document ID it sees.
This anonymization step is stable over time, because it always uses the same cryptographic salt
(stored in your PHI folder).

When it sees a match for one of the anonymous docref IDs you gave in the `docref_id` column
(among all the patient's documents), it will download that document.

### Saving the Selected Documents

It might be useful to save the exported documents from the EHR
(or even the smaller selection from a giant ndjson folder),
for faster iterations of the chart review mode or
just confirming the correct documents were chosen.

Pass in an argument like `--export-to /in/export` to save the ndjson for the selected documents
in the given folder. (Note this does not save the clinical note text unless it is already inline
-- this is just saving the DocumentReference resources).

## Custom NLP Dictionaries

If you would like to customize the dictionary terms that are marked up and sent to Label Studio,
simply pass in a new dictionary like so: `--symptoms-bsv /in/my-symptoms.bsv`.

This file should look like (this is a portion of the default Covid dictionary):
```
##  Columns = CUI|TUI|CODE|SAB|STR|PREF
##      CUI = Concept Unique Identifier
##      TUI = Type Unique Identifier
##     CODE = Vocabulary Code
##      SAB = Vocabulary Source Abbreviation (SNOMEDCT_US)
##      STR = String text in clinical note (case insensitive)
##     PREF = Preferred output concept label

##  Congestion or runny nose
C0027424|T184|68235000|SNOMEDCT_US|nasal congestion|Congestion or runny nose
C0027424|T184|68235000|SNOMEDCT_US|stuffed-up nose|Congestion or runny nose
C0027424|T184|68235000|SNOMEDCT_US|stuffy nose|Congestion or runny nose
C0027424|T184|68235000|SNOMEDCT_US|congested nose|Congestion or runny nose
C1260880|T184|64531003|SNOMEDCT_US|rhinorrhea|Congestion or runny nose
C1260880|T184|64531003|SNOMEDCT_US|Nasal discharge|Congestion or runny nose
C1260880|T184|64531003|SNOMEDCT_US|discharge from nose|Congestion or runny nose
C1260880|T184|267101005|SNOMEDCT_US|nose dripping|Congestion or runny nose
C1260880|T184|267101005|SNOMEDCT_US|nose running|Congestion or runny nose
C1260880|T184|267101005|SNOMEDCT_US|running nose|Congestion or runny nose
C1260880|T184|HP:0031417|HPO|runny nose|Congestion or runny nose
C0027424|T184|R09.81|ICD10CM|R09.81|Congestion or runny nose

##  Diarrhea
C0011991|T184|62315008|SNOMEDCT_US|diarrhea|Diarrhea
C0011991|T184|R19.7|ICD10CM|R19.7|Diarrhea
C0011991|T184|HP:0002014|HPO|Watery stool|Diarrhea
C0011991|T184|HP:0002014|HPO|Watery stools|Diarrhea
```

Chart review mode will only label phrases whose CUI appears in this symptom file.
And the label used will be the last part of each line (the `PREF` part).

That is, with the above symptoms file, the word `headache` would not be labelled at all
because no CUI for headache is listed in the file.
But any phrases that cTAKES tags as CUI `C0011991` will be labelled as `Diarrhea`.

cTAKES has an internal dictionary and knows some phrases already.
But you may get better results by adding extra terms and variations in your symptoms file.

## Disabling Features

You may not need NLP or `philter` processing.
Simply pass `--no-nlp` or `--no-philter` and those steps will be skipped.

## Label Studio

### Overwriting

By default, chart review mode will never overwrite any data in Label Studio.
It will push new notes and skip any that were already uploaded to Label Studio.

But obviously, that becomes annoying if you are iterating on a dictionary or
otherwise re-running chart review mode.

So to overwrite existing notes, simply pass `--overwrite`.

### Label Config

Before using chart review mode, you should have already set up your Label Studio instance.
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

Or you can use dynamic labels, and chart review mode will define them from your symptoms file:
```
<View>
  <Labels name="label" toName="text" value="$label" />
  <Text name="text" value="$text"/>
</View>
```
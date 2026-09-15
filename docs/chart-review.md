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
Along the way, it can pre-mark the note with labels and/or anonymize the note with
[philter](https://github.com/SironaMedical/philter-lite).

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
1. It extracts DiagnosticReport and/or DocumentReference resources from a folder of NDJSON.
2. It transforms the contained notes via `philter` (and optionally labeling it).
3. It loads the results into Label Studio.

### Minimal Command Line

Upload mode takes two main arguments:
1. Input path (local dir of NDJSON)
2. PHI/build path (the same PHI/build path you normally provide to Cumulus ETL)

Additionally, there are three Label Studio parameters:
1. `--label-studio-url URL` (the address of your Label Studio server)
2. `--ls-token PATH` (a file holding your Label Studio authentication token)
3. `--ls-project ID` (the number of the Label Studio project you want to push notes to)

(All three are only needed when actually uploading —
see [Skipping the Upload](#skipping-the-upload).)

Taken altogether, here is an example minimal `upload-notes` command:
```sh
docker compose run --rm \
 --volume /local/path:/host \
 cumulus-etl upload-notes \
  --label-studio-url https://my-label-studio-server/ \
  --ls-token /host/label-studio-token.txt \
  --ls-project 3 \
  --s3-region=us-east-2 \
  /host/my-input-folder/ \
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

### Capping the Number of Charts

Sometimes you have more charts selected than you actually want to review
(for example, you oversampled for an earlier NLP stage but only want a fixed number for
an inter-rater agreement study).

Pass `--count=N` to upload at most `N` charts.
If more than `N` charts are available, a random sample of `N` is uploaded.
The count is applied *after* grouping, so it counts the final Label Studio charts — with the
default encounter grouping, one chart may contain several notes. (Use `--grouping=none` if you
want the count to apply to individual notes.)

Add `--seed=NUMBER` for a reproducible sample.

If you'd rather sample separately, the standalone `sample` command can produce a `.csv` that you
feed back in with `--select-by-csv`. (Note that `sample` selects individual notes, whereas
`--count` here selects grouped charts.)

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

When you use `--export-to`, upload mode also writes an `uploaded_notes.csv` manifest into that
folder, recording exactly which notes were uploaded (both real and anonymized note IDs, plus
patient and encounter IDs). This is handy for auditing a capped upload, and the `note_ref` column
can be fed straight back into a later run with `--select-by-csv`.

## Pre-Labeling Notes

You may want to make the human chart reviewer's life easier by pre-labeling or pre-annotating
the note text before they see it.

You can manually highlight some terms by passing `--highlight-by-word` or `--highlight-by-regex`.

You can also add more complicated labels (highlighting but with a study tag)
by passing `--label-by-csv`, `--label-by-anon-csv`, or `--label-by-athena-table`.
These all expect a certain format:
- A note ID column (`note_ref`, `documentreference_id`, `diagnosticreport_ref`, etc)
- A `label` column holding names of Label Studio labels
- A `span` column holding spans of the note to highlight like `124:157`
- Optionally `sublabel_name` and `sublabel_value` columns if the Label Studio label has more
  complicated sub-options.
- Optionally an `origin` column that will name the source of the labels (used to separate
  labels into separate annotation sources in Label Studio)

## Exporting Labels for Chart Review

Once your reviewers have annotated the charts, you'll want to measure how well the NLP agreed
with them, using [Chart Review](https://docs.smarthealthit.org/cumulus/chart-review/).

Pass `--export-labels-to PATH` and upload mode will write those CSVs for the charts it prepared:
```sh
docker compose run --rm \
 --volume /local/path:/host \
 cumulus-etl upload-notes \
  --label-studio-url https://my-label-studio-server/ \
  --ls-token /host/label-studio-token.txt \
  --ls-project 3 \
  --label-by-athena-table my_study__nlp_labels \
  --export-labels-to /host/chart-review-project/ \
  /host/my-input-folder/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir/
```

You'll get one `labels-<origin>.csv` per label origin,
since Chart Review scores one annotator per file.
To compare several models, run upload mode once per model,
then point a single Chart Review config at all the resulting files:
```yaml
annotators:
  me: 1
  gpt-oss-120b:
    filename: labels-gpt-oss-120b.csv
  claude-sonnet45:
    filename: labels-claude-sonnet45.csv
```

Pairing this with [`--export-to`](#saving-the-selected-documents) is often handy:
that writes an `uploaded_notes.csv` manifest listing exactly which notes went up,
which is the quickest way to confirm the label files cover the charts you expect.

A few things worth knowing about the generated files:
- Notes that an origin didn't label still get a row, with an empty label.
  Chart Review reads that as "reviewed, found nothing" — leaving the row out would mean
  "not reviewed", which would shrink that annotator's denominator.
- The `note_ref` column holds fully-qualified references, so DocumentReferences and
  DiagnosticReports sit in the same file. That column can also be fed straight back into
  `--select-by-csv` to re-run against the same notes.
- Labels containing a `|` are left out, since Chart Review reserves it as its own
  label/sublabel delimiter and won't load a file containing one.
- If one annotator uses a sublabel for a label and another uses it bare, Chart Review discards
  *all* the bare mentions of that label, from every annotator — human reviewers included.
  Upload mode warns when it notices this, but it's best to settle on one or the other.

### Skipping the Upload

If you only want the label files — say you're scoring another model against charts you uploaded
earlier — pass `--no-upload`.
Upload mode will prepare everything and write the local files without contacting Label Studio,
so you can leave out the Label Studio URL, `--ls-project`, and `--ls-token` entirely:
```sh
docker compose run --rm \
 --volume /local/path:/host \
 cumulus-etl upload-notes \
  --no-upload \
  --label-by-athena-table my_study__nlp_labels \
  --export-labels-to /host/chart-review-project/ \
  /host/my-input-folder/ \
  s3://my-cumulus-prefix-phi-99999999999-us-east-2/subdir/
```

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

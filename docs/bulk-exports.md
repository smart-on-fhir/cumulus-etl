---
title: Bulk Exports
parent: ETL
nav_order: 9
# audience: engineer familiar with the project
# type: howto
---

# Bulk FHIR Exports

Cumulus ETL wants data, and lots of it.

It's happy to ingest data that you've gathered elsewhere (as a separate export),
but it's also happy to download the data itself as needed during the ETL (as an on-the-fly export).

## Export Options

### External Exports

1. If you have an existing process to export health data, you can do that bulk export externally,
and then just feed the resulting files to Cumulus ETL.
(Though note that you will need to provide some export information manually,
with the `--export-group` and `--export-timestamp` options. See `--help` for more info.)

2. Cumulus ETL has an `export` command to perform just a bulk export without an ETL step.
   Run it like so: `cumulus-etl export FHIR_URL ./output` (see `--help` for more options).
   - You can provide standard
   [bulk FHIR options](https://hl7.org/fhir/uv/bulkdata/export.html#query-parameters)
   like `_type` and `_typeFilter` in the URL or via CLI arguments like
   `--type` and `--type-filter`.
   - This workflow will generate an export log file, from which Cumulus ETL can pull
   some export metadata like the Group name and export timestamp.

3. Or you may need more advanced options than our internal exporter supports.
   The [SMART Bulk Data Client](https://github.com/smart-on-fhir/bulk-data-client)
   is a great tool with lots of features (and also generates an export log file).

In any case, it's simple to feed that data to the ETL:
1. Pass Cumulus ETL the folder that holds the downloaded data as the input path.
1. Pass `--fhir-url=` pointing at your FHIR server so that externally referenced document notes
   and medications can still be downloaded as needed.

### On-The-Fly Exports

If it's easier to just do it all in one step,
you can also start an ETL run with your FHIR URL as the input path.
Cumulus ETL will do a bulk export first, then ETL the results.

You can save the exported files for archiving after the fact with `--export-to=PATH`.

However, bulk exports tend to be brittle and slow for many EHRs at the time of this writing.
It might be wiser to separately export, make sure the data is all there and good, and then ETL it.

## Cumulus Assumptions

Cumulus ETL makes some specific assumptions about the data you feed it and the order you feed it in.

This is because Cumulus tracks which resources were exported from which FHIR Groups and when.
It only allows Encounters that have had all their data fully imported to be queried by SQL,
to prevent an in-progress ETL workflow from affecting queries against the database.
(i.e. to prevent an Encounter that hasn't yet had Conditions loaded in from looking like an
Encounter that doesn't _have_ any Conditions)

Of course, even in the normal course of events, resources may show up weeks after an Encounter
(like lab results).
So an Encounter can never knowingly be truly _complete_,
but Cumulus ETL makes an effort to keep a consistent view of the world at least for a given
point in time.

### Encounters First

**Please export Encounters along with or before you export other Encounter-linked resources.**
(Patients can be exported beforehand, since they don't depend on Encounters.)

To prevent incomplete Encounters, Cumulus only looks at Encounters that have an export
timestamp at the same time or before linked resources like Condition.
(As a result, there may be extra Conditions that point to not-yet-loaded Encounters.
But that's fine, they will also be ignored until their Encounters do get loaded.)

If you do export Encounters last, you may not see any of those Encounters in the `core` study
tables once you run Cumulus Library on the data.
(Your Encounter data is safe and sound,
just temporarily ignored by the Library until later exports come through.)

### No Partial Group Exports

**Please don't slice and dice your Group resources when exporting.**
Cumulus ETL assumes that when you feed it an input folder of export files,
that everything in the Group is available (at least, for the exported resources).
You can export one resource from the Group at a time, just don't slice that resource further.

This is because when you run ETL on say, Conditions exported from Group `Group1234`,
it will mark Conditions in `Group1234` as completely loaded (up to the export timestamp).

Using `_since` or a date-oriented `_typeFilter` is still fine, to grab new data for an export.
The concern is more about an incomplete view of the data at a given point in time.

For example, if you sliced Conditions according to category when exporting
(e.g. `_typeFilter=Condition?category=problem-list-item`),
Cumulus will have an incorrect view of the world
(thinking it got all Conditions when it only got problem list items).

You can still do this if you are careful!
For example, maybe exporting Observations is too slow unless you slice by category.
Just make sure that after you export all the Observations separately,
you then combine them again into one big Observation folder before running Cumulus ETL.

## Archiving Exports

Exports can take a long time, and it's often convenient to archive the results.
For later re-processing, sanity checking, quality assurance, or whatever.

It's recommended that you archive everything in the export folder.
This is what you may expect to archive:

- The resource export files themselves
  (these will look like `1.Patient.ndjson` or `Patient.000.ndjson` or similar)
- The `log.ndjson` log file
- The `deleted/` subfolder, if present
  (this will hold a list of resources that the FHIR server says should be deleted)
- The `error/` subfolder, if present
  (this will hold a list of errors from the FHIR server
  as well as warnings and informational messages, despite the name)

## Downloading Clinical Notes Ahead of Time

If you are interested in running NLP tasks,
that will usually involve downloading a lot of clinical note attachments found
in DocumentReferences (and/or DiagnosticReports).

Since EHRs can be a little flaky, old attachment URLs may move,
and/or in case you later lose access to the EHR,
it is recommended that you download the attachments ahead of time and only once by _inlining_ them.

### What's Inlining?

Inlining is the process of taking an original NDJSON attachment definition like this:
```json
{
  "url": "https://example.com/Binary/document123",
  "contentType": "text/html"
}
```

Then downloading the referenced URL,
and stuffing the results back into the NDJSON with some extra metadata like so:
```json
{
  "url": "https://example.com/Binary/document123",
  "contentType": "text/html; charset=utf8",
  "data": "aGVsbG8gd29ybGQ=",
  "size": 11,
  "hash": "Kq5sNclPz7QV2+lfQIuc6R7oRu0="
}
```

Now the data is stored locally in your downloaded NDJSON
and can be processed independently of the EHR.

### How to Inline

Cumulus ETL has a special inlining mode.
Simply run the following command,
pointing at both a source NDJSON folder and your EHR's FHIR URL.

```shell
cumulus-etl inline ./ndjson-folder FHIR_URL --smart-client-id XXX --smart-key /YYY
```

{: .note }
This will modify the data in the input folder!

By default, this will inline text, HTML, and XHTML attachments
for any DiagnosticReports and DocumentReferences found.
But there are options to adjust those defaults.
See `--help` for more information.

If you are using Cumulus ETL to do your bulk exporting,
you can simply pass `--inline` to the export command (see `--help` for more inlining options)
in order to inline as part of the bulk export process.

## Resuming an Interrupted Export

Bulk exports can be brittle.
The server can give the odd occasional error or time you out.
Maybe you lose your internet connection.
Who knows.

But thankfully, you can resume a bulk export with Cumulus ETL.
Every time you start a bulk export,
the ETL will print an argument that you can add onto your command line
if you get interrupted and want to try again.

When you add this command line argument (it usually looks like `--resume=URL`),
the rest of the bulk export arguments (like `--since`) that you provide don't matter,
but they are also harmless and will be ignored.

## Registering an Export Client

On your server, you need to register a new "backend service" client.
You'll be asked to provide some sort of private/public key.
See below for generating that.
You'll also be asked for a client ID or the server may generate a client ID for you.

### Generating a JWK Set

A JWK Set (JWKS) is just a file with some cryptographic keys,
usually holding a public and private version of the same key.
FHIR servers use it to grant clients access.

You can generate a JWKS using the RS384 algorithm and a random ID by running the command below.

(Make sure you have `jose` installed first.)

```sh
jose jwk gen -s -i "{\"alg\":\"RS384\",\"kid\":\"`uuidgen`\"}" -o private.jwks
jose jwk pub -s -i private.jwks -o public.jwks
```

After giving `public.jwks` to your FHIR server,
you can pass `private.jwks` to Cumulus ETL with `--smart-key` (example below).

### Generating a PEM key

A PEM key is just a file with a single private cryptographic key.
Some FHIR servers may use it to grant clients access.

If your FHIR server uses a PEM key,
it will provide instructions on the kind of key it expects and how to generate it.
See for example,
[Epic's documentation](https://vendorservices.epic.com/Article?docId=oauth2&section=Creating-Key-Pair).

After giving the public key to your FHIR server,
you can pass your `private.pem` file to Cumulus ETL with `--smart-key` (example below).

### SMART Arguments

You'll need to pass two arguments to Cumulus ETL:

```sh
--smart-client-id=YOUR_CLIENT_ID
--smart-key=/path/to/private.jwks
```

You can also give `--smart-client-id` a path to a file with your client ID,
if it is too large and unwieldy for the commandline.

And for Cumulus ETL's input path argument,
you will give your server's URL address,
including a Group identifier if you want to scope the export
(e.g. `https://example.com/fhir` or `https://example.com/fhir/Group/1234`).

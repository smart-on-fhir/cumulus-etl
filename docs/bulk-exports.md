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

## Separate Exports

1. If you have an existing process to export health data, you can do that bulk export externally,
and then just feed the resulting files to Cumulus ETL.

2. Cumulus ETL has an `export` command to perform just a bulk export without an ETL step.
   Run it like so: `cumulus-etl export FHIR_URL ./output` (see `--help` for more options).
   You can use all sorts of
   [interesting FHIR options](https://hl7.org/fhir/uv/bulkdata/export.html#query-parameters)
   like `_typeFilter` or `_since` in the URL.

3. Or you may need more advanced options than our internal exporter supports.
   The [SMART Bulk Data Client](https://github.com/smart-on-fhir/bulk-data-client)
   is a great tool with lots of features.

In any case, it's simple to feed that data to the ETL:
1. Pass Cumulus ETL the folder that holds the downloaded data as the input path.
1. Pass `--fhir-url=` pointing at your FHIR server so that externally referenced document notes
   and medications can still be downloaded as needed.

## On-The-Fly Exports

If it's easier to just do it all in one step,
you can also start an ETL run with your FHIR URL as the input path.
Cumulus ETL will do a bulk export first, then ETL the results.

You can save the exported files for archiving after the fact with `--export-to=PATH`.

However, bulk exports tend to be brittle and slow for many EHRs at the time of this writing.
It might be wiser to separately export, make sure the data is all there and good, and then ETL it.

## Registering an Export Client

On your server, you need to register a new "backend service" client.
You'll be asked to provide a JWKS (JWK Set) file.
See below for generating that.
You'll also be asked for a client ID or the server may generate a client ID for you.

### Generating a JWKS

A JWKS is just a file with some cryptographic keys,
usually holding a public and private version of the same key.
FHIR servers use it to grant clients access.

You can generate a JWKS using the RS384 algorithm and a random ID by running the command below.

(Make sure you have `jose` installed first.)

```sh
jose jwk gen -s -i "{\"alg\":\"RS384\",\"kid\":\"`uuidgen`\"}" -o rsa.jwks
```

Then give `rsa.jwks` to your FHIR server and to Cumulus ETL (details on that below).

### SMART Arguments

You'll need to pass two new arguments to Cumulus ETL: 

```sh
--smart-client-id=YOUR_CLIENT_ID
--smart-jwks=/path/to/rsa.jwks
```

You can also give `--smart-client-id` a path to a file with your client ID,
if it is too large and unwieldy for the commandline.

And for Cumulus ETL's input path argument,
you will give your server's URL address,
including a Group identifier if you want to scope the export
(e.g. `https://example.com/fhir` or `https://example.com/fhir/Group/1234`).

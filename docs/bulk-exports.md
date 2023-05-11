---
title: Bulk Exports
parent: ETL
nav_order: 9
# audience: engineer familiar with the project
# type: howto
---

# Bulk FHIR Exports

Cumulus ETL wants data, and lots of it.

It's happy to ingest data that you've gathered elsewhere (what we call external exports),
and it's happy to download the data itself (internal exports).

## External Exports

If you have an existing process to export health data, you can do that bulk export externally,
and then just feed the resulting files to Cumulus ETL.

Or you may need more export options than our internal exporter supports.
The [SMART Bulk Data Client](https://github.com/smart-on-fhir/bulk-data-client)
is a great tool with lots of features.

In either case, it's simple to feed that data to the ETL:
1. Pass Cumulus ETL the folder that holds the downloaded data as the input path.
1. Pass `--fhir-url=` pointing at your FHIR server so that external document notes can be downloaded.

## Internal Exports

If you don't have an existing process or you don't need too many fancy options,
Cumulus ETL's internal bulk exporter can do the trick. 

### Registering Cumulus ETL

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

### Narrowing Export Scope

You can pass `--since=` and/or `--until=` to narrow your bulk export to a date range.

Note that support for these parameters among EHRs is not super common.
- `--since=` is in the FHIR spec but is not required by law.
  (And notably, it's not supported by Epic.)
- `--until=` is not even in the FHIR spec yet. No major EHR supports it.

But if you are lucky enough to be working with an EHR that supports either one,
you can pass in a time like `--since=2023-01-16T20:32:48Z`.

### Saving Bulk Export Files

Bulk exports can be tricky to get right and can take a long time.
Often (and especially when first experimenting with Cumulus ETL),
you will want to save the results of a bulk export for inspection or in case Cumulus ETL fails.

By default, Cumulus ETL throws away the results of a bulk export once it's done with them.
But you can pass `--export-to=/path/to/folder` to instead save the exported `.ndjson` files in the given folder.

Note that you'll want to expose the local path to docker so that the files reach your actual disk, like so:

```sh
docker compose \
  run --rm \
  --volume /my/exported/files:/folder \
  cumulus-etl \
  --export-to=/folder \
  https://my-fhir-server/ \
  s3://output/ \
  s3://phi/
```

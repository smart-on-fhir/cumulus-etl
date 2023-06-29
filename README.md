# Cumulus ETL

[Cumulus](https://docs.smarthealthit.org/cumulus/)
is an entire healthcare pipeline for population-scale clinical investigations.

Cumulus ETL is the first critical piece of that pipeline.
- It **extracts** bulk patient data from your EHR.
- It **transforms** that data by anonymizing it and running NLP on clinical notes
- It **loads** that data onto the cloud to be queried by
  [Cumulus Library](https://github.com/smart-on-fhir/cumulus-library) SQL

## Documentation

For guides on installing & using Cumulus ETL,
[read our documentation](https://docs.smarthealthit.org/cumulus/etl/).

## Example

A simple run of Cumulus ETL might look something like:
```shell
docker compose run \
  cumulus-etl \
  s3://my-input-bucket/bulk-export/ \
  s3://my-output-bucket/delta-lakes/ \
  s3://my-phi-bucket/build-and-phi-artifacts/
```

This line would read ndjson files from the input bucket,
drop the result as [Delta Lakes](https://delta.io/) into the output bucket,
and save some bookkeeping configuration to a build/phi bucket.

## Contributing

We love 💖 contributions!

If you have a good suggestion 💡 or found a bug 🐛,
[read our brief contributors guide](CONTRIBUTING.md)
for pointers to filing issues and what to expect.

If you're a programmer ⌨ and are looking for a starting place to help, we keep a
[list of good bite-size issues](https://github.com/smart-on-fhir/cumulus-etl/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
for first-time contributions.

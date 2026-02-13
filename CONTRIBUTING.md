# How to contribute to Cumulus ETL

## Did you find a bug?

Excellent!

Please first look at the
[existing GitHub issues](https://github.com/smart-on-fhir/cumulus-etl/issues)
to see if it has already been reported.
But if you don't see your bug, please
[file a new GitHub issue](https://github.com/smart-on-fhir/cumulus-etl/issues/new),
and we will look at it as soon as we can.  

The more detail you can add, the quicker we will be able to fix the bug.

## Do you want to write a patch that fixes a bug?

Even better! Thank you!

### Set up your dev environment

To use the same dev environment as us, you'll want to run these commands:
```sh
pip install .[dev]
pre-commit install
```

This will install the pre-commit hooks for the repo (which automatically enforce styling for you).

### Running unit tests

1. First, you'll want to install a Java JDK, for Delta Lake support.

2. Then just run `pytest`.
All dependencies should have been installed by the `pip install .[dev]` above.

### How to show us the patch

Open a new GitHub PR and one of the maintainers will notice it and comment.

Please add as much detail as you can about the problem you are solving and ideally even link to
the related existing GitHub issue.

### What to expect

All code changes (even changes made by the main project developers)
go through several automated CI steps and a manual code review.

#### Automatic CI

Here's what GitHub will automatically run:
- unit tests (run `pytest` locally to confirm your tests pass)
- lint tests (run `pylint cumulus_etl tests` to confirm your code passes)
- security static analysis (via `bandit`)

#### Manual review

A project developer will also review your code manually.
Every PR needs one review approval to land.

A reviewer will be looking for things like:
- suitability (not every change makes sense for the project scope or direction)
- maintainability
- general quality

Once approved, you can merge your PR yourself as long as the other GitHub tests pass.
Congratulations, and thank you!

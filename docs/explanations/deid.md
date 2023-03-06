<!-- Target audience: non-programmers, direct tone -->

# How Does Cumulus De-identify Patient Data?

First, let's review the timeline of when PHI gets redacted as
patient data flows through the Cumulus pipeline.
And then we'll discuss which specific fields gets de-identified and how.

## The PHI Lifecycle

There are three main stages of patient data anonymity,
as the Cumulus project shepherds that data from the EHR to the Cumulus dashboard:

1. Full PHI records
2. De-identified records
3. Patient counts

### Full PHI

This is the raw data from the EHR, usually in the form of a bulk FHIR export.
Cumulus ETL saves this data locally to a temporary folder before beginning its work
(this folder gets deleted after use or even if the program is interrupted).

### De-identified Records

Cumulus ETL runs its de-identification routine (see below) & natural language processing (NLP)
and then uploads the resulting de-identified records to an output S3 bucket.
Note this is all still inside your own IT infrastructure.

### Patient Counts

Each study will have its own SQL queries that run against the de-identified records.
(And only the de-identified records,
as these queries have no access to the full PHI at this point in the pipeline.)

These SQL queries will all result in a simple count of the target information.
That might be how many patients had a particular symptom, how many patients _didn't_,
how many patients are prescribed a particular medication, etc.

For example, a Covid study might query how many patients have fever symptoms,
with a result like "10,000 patients showed fever symptoms on 10/15/2021."
And that count would be sent on to the Cumulus dashboard.

But no specific patient data can be sent to the dashboard.
Just the total count of results of a given SQL query.

This is the first time data leaves your institution, and by this time,
there is no PHI at all. Just counts.

## De-identification

The full PHI and patient count stages are easy to understand.
All the PHI or none of it.

But the piece in-between where de-identified data sits at rest in Amazon S3 is more nuanced.
Let's explore that.

There are three main transformations of PHI inside Cumulus ETL:
1. A [Microsoft anonymization tool](https://github.com/microsoft/Tools-for-Health-Data-Anonymization)
   is run on the bulk data.
2. Cumulus ETL replaces all resource IDs with anonymized IDs
3. NLP is run on physician notes (which are then discarded)

### Microsoft Anonymizer

This is a standard tool for anonymization of FHIR resources.
But the devil is in the details, because it can be configured to do nothing at all
or redact everything.

Cumulus ETL uses a
[custom configuration](https://github.com/smart-on-fhir/cumulus-etl/blob/main/cumulus/deid/ms-config.json),
based off the
[standard recommended HIPAA config](https://github.com/microsoft/Tools-for-Health-Data-Anonymization/blob/master/FHIR/src/Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool/configuration-sample.json).

#### The Standard Config

The base configuration does a few common but interesting things:
- Removes a lot of the standard identifying elements like names, addresses, etc.
- Date-shifts any dates or times (and redacts ages to birth-year and marks anyone over 89 as 89)
- Strips zip codes down to three digits, plus entirely redacts a list of small-population zip codes
  where even three digits is too identifying

Now let's explore the exceptions to the base configuration.

#### Dates Are Not Shifted

Accurate dates are too useful for study purposes to randomly shift a month here or there.
And they carry
[minimal PHI risk](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3907029/).

Birthdays remain redacted to birth-year.
And 89+ remains a single age group.

#### Other Useful Fields Are Kept

Like dates, some FHIR fields are kept because they are useful for studies:
- Race
- Codings of all sorts (UMLS, SNOMED, etc markings)
- IDs (see below for how these are handled)
- Physician notes (see below for how these are handled)

### IDs

Cumulus ETL de-identifies FHIR resource IDs itself.

By IDs, we are only talking about
[FHIR resource IDs](https://www.hl7.org/fhir/resource-definitions.html#Resource.id).
Other identifiers (like
[patient identifiers](https://www.hl7.org/fhir/patient-definitions.html#Patient.identifier))
are always stripped out entirely.

These resource IDs are one-way securely hashed for anonymity.
This is the same algorithm that Microsoft's tool uses, but with even more entropy.
(Specifically, Cumulus uses the HMAC-SHA256 hash with a 256 bit salt.)

#### Patients and Encounters

Patient and Encounter resources are anonymized like any other resource.
But with one difference.

A mapping from the old to the new IDs is kept for debugging purposes.
If there is ever a concern about data integrity or oddities are observed in the
de-identified results, it is crucial that some mechanism exists to reverse the
anonymization so that your institution can investigate.

This mapping is obviously very precious and is treated as sensitive PHI.
It's stored in a special PHI directory (the third argument to Cumulus ETL).
And you control where that PHI directory lives (an S3 bucket, a local disk, etc.),
so that it can be locked down as tightly as you like.
It never leaves your institution's control.

Any other resource is usually already tied to a patient or encounter.
So Cumulus does not bother keeping a mapping for those.

### Freeform Text Fields

There are some freeform text fields that Cumulus ETS asks the Microsoft Anonymizer tool to leave in.
These fields are useful for presenting or computing a phenotype:
- `CodeableConcept.text`
- `Coding.display`
- `Observation.valueString` and `Observation.component.valueString`

Although Cumulus wants to largely preserve these fields,
they may contain PHI since they are freeform text fields after all.

If that is likely for your institution, you can have Cumulus ETL run
[philter](https://github.com/SironaMedical/philter-lite) over these freeform fields, by passing `--philter`.
This replaces any detected PHI like names, phone numbers, MRNs, social security numbers, etc. with asterisks.

But be warned that it will significantly slow down the ETL process.

### NLP on Physician Notes

Physician notes are kept long enough to run them through cTAKES natural language processing,
then they are thrown away.

The resulting detected symptoms and other medical codes from cTAKES are then kept in the de-identified results.

There is a theoretical risk that cTAKES may mis-identify PHI as a medical term.
As an extremely contrived example of a false positive:
"Referring to Dr. Anosmia Jones. Patient has Anosmia as a friend."

But even in such cases, there is no stored context for a medical term's usage.
That is, the surrounding text is not stored, and the word will simply appear to be a false positive symptom.
So if that does happen, it would be more of a quality concern than a PHI concern.

## Conclusion

And that's it!
In summation, the only data that leaves your institution are just raw counts that could not be considered PHI.

But inside your institution, there is some de-identified resources that sticks around
as well as more sensitive ID mappings for patients and encounters.

Hopefully you feel a little more at ease about how de-identification is performed,
but always feel free to reach out to the Cumulus team for suggestions or questions.

<!-- Target audience: engineer familiar with the project, helpful direct tone -->

# Epic Tips & Tricks

## Frequent Bulk Exporting

You may encounter this error:
`Error processing Bulk Data Kickoff request: Request not allowed: The Client requested this Group too recently.`.

If so, you will want to update the `FHIR_BULK_CLIENT_REQUEST_WINDOW_TBL` to a longer time.
The default is 24 hours.

## Long IDs

In rare cases, Epic's bulk FHIR export can generate IDs that are longer than the mandated 64-character limit.
Cumulus ETL itself will not mind this, but if you use another bulk export client, you may find that it complains. 
If so, you have to reach out to Epic to cap it at 64 characters.

## Batch Updates

Epic has not yet (as of early 2023) implemented the `_since` or `_typeFilter` parameters for bulk exports.
This means you have no easy way to dynamically limit the scope of a given export to a date range.

Instead, you'll need to scope your registry definition to a specific date range.
Then whenever you want to export a new date range, just edit your registry before exporting.

### Initial Import

For performance reasons, you don't want to try to export your whole history in one go.

Instead, you should add a date range to your registry and start at the earliest date possible.
Run Cumulus ETL and once you confirm it works, slide your registry range forward.
Rinse and repeat until you have imported all your data.

### Incremental Updates

When running Cumulus on a regular ongoing basis,
make sure you update your registry to slide your date range forward before running Cumulus ETL.

## API Access

Epic will ask you for a list of APIs / resources to enable.
Here is the currently recommended list to request.
If you have suggestions for edits, please send them in.

- Binary.Read (Clinical Notes) (R4)
- Bulk Data Delete Request
- Bulk Data File Request
- Bulk Data Kickoff
- Bulk Data Status Request
- Condition.Search (Encounter Diagnosis) (R4)
- Condition.Search (Infection) (R4)
- Condition.Search (Medical History) (R4)
- Condition.Search (Problems) (R4)
- Condition.Search (Reason for Visit) (R4)
- DocumentReference.Search (Clinical Notes) (R4)
- Encounter.Search (R4)
- MedicationRequest.Search (Orders) (R4)
- Observation.Search (Labs) (R4)
- Observation.Search (Social History) (R4)
- Observation.Search (Vitals) (R4)
- Patient.$match
- Patient.Search (R4)

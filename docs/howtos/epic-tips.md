<!-- Target audience: engineer familiar with the project, helpful direct tone -->

# Epic Tips & Tricks

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
- Binary.Read (Clinical Reference) (R4)
- Binary.Read (Correspondences) (R4)
- Binary.Read (Document Information) (R4)
- Binary.Read (External CCDA Document) (R4)
- Binary.Read (Handoff) (R4)
- Binary.Read (HIS) (R4)
- Binary.Read (IRF-PAI) (R4)
- Binary Read (Labs) (R4)
- Binary.Read (MDS) (R4)
- Binary.Read (OASIS) (R4)
- Binary.Read (Patient-Entered Questionnaires) (R4)
- Binary.Read (Radiology Results) (R4)
- Bulk Data Delete Request
- Bulk Data File Request
- Bulk Data Kickoff
- Bulk Data Status Request
- Condition.Read (Encounter Diagnosis) (R4)
- Condition.Read (Infection) (R4)
- Condition.Read (Medical History) (R4)
- Condition.Read (Problems) (R4)
- Condition.Read (Reason for Visit) (R4)
- Condition.Search (Encounter Diagnosis) (R4)
- Condition.Search (Infection) (R4)
- Condition.Search (Medical History) (R4)
- Condition.Search (Problems) (R4)
- Condition.Search (Reason for Visit) (R4)
- DocumentReference.Read (Clinical Notes) (R4)
- DocumentReference.Search (Clinical Notes) (R4)
- Encounter.Read (R4)
- Encounter.Search (R4)
- MedicationRequest.Read (Orders) (R4)
- MedicationRequest.Search (Orders) (R4)
- Observation.Read (Labs) (R4)
- Observation.Read (Social History) (R4)
- Observation.Read (Vitals) (R4)
- Observation.Search (Labs) (R4)
- Observation.Search (Social History) (R4)
- Observation.Search (Vitals) (R4)
- Patient.$match
- Patient.Read (R4)
- Patient.Search (R4)

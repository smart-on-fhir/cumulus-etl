"""Standard FHIR resource tasks"""

import copy
import logging
import os

import rich.progress

from cumulus_etl import common, fhir, store
from cumulus_etl.etl import tasks


class AllergyIntoleranceTask(tasks.EtlTask):
    name = "allergyintolerance"
    resource = "AllergyIntolerance"
    tags = {"cpu"}


class ConditionTask(tasks.EtlTask):
    name = "condition"
    resource = "Condition"
    tags = {"cpu"}


class DeviceTask(tasks.EtlTask):
    name = "device"
    resource = "Device"
    tags = {"cpu"}


class DiagnosticReportTask(tasks.EtlTask):
    name = "diagnosticreport"
    resource = "DiagnosticReport"
    tags = {"cpu"}


class DocumentReferenceTask(tasks.EtlTask):
    name = "documentreference"
    resource = "DocumentReference"
    tags = {"cpu"}


class EncounterTask(tasks.EtlTask):
    name = "encounter"
    resource = "Encounter"
    tags = {"cpu"}


class ImmunizationTask(tasks.EtlTask):
    name = "immunization"
    resource = "Immunization"
    tags = {"cpu"}


class MedicationRequestTask(tasks.EtlTask):
    """Write MedicationRequest resources and associated Medication resources"""

    name = "medicationrequest"
    resource = "MedicationRequest"
    tags = {"cpu"}

    # We may write to a second Medication table as we go.
    # MedicationRequest can have inline medications via CodeableConcepts, or external Medication references.
    # If external, we'll download them and stuff them in this output table.
    # We do all this special business logic because Medication is a special, "reference" resource,
    # and many EHRs don't let you simply bulk export them.

    outputs = [
        tasks.OutputTable(),
        tasks.OutputTable(name="medication", schema="Medication"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Keep a cache of medication IDs that we've already downloaded.
        # If this ends up growing too large in practice, we can wipe it during a call to table_batch_cleanup().
        # But let's try initially with keeping it around for the whole task.
        self.medication_ids = set()

    def scrub_medication(self, medication: dict | None) -> bool:
        """Scrub incoming medication resources, returns False if it should be skipped"""
        if not medication or not self.scrubber.scrub_resource(medication):  # standard scrubbing
            return False

        # Normally the above is all we'd need to do.
        # But this resource just came hot from the FHIR server, and we did not run the MS anonymizer on it.
        # Since Medications are not patient-specific, we don't need the full MS treatment.
        # But still, we should probably drop some bits that might more easily identify the *institution*.
        # This is a poor-man's MS config tool (and a blocklist rather than allow-list, but it's a very simple resource)
        medication.pop("extension", None)  # *should* remove at all layers, but this will catch 99% of them
        medication.pop("identifier", None)
        medication.pop("text", None)
        # Leave batch.lotNumber freeform text in place, it might be useful for quality control

        return True

    async def fetch_medication(self, resource: dict) -> dict | None:
        """Downloads an external Medication if necessary"""
        reference = resource.get("medicationReference", {}).get("reference")
        if not reference:
            return None

        if not reference.startswith("#"):
            # Don't duplicate medications we've already seen this run.
            # This will still duplicate medications from previous runs, but avoiding that feels like more work than it's
            # worth - just download em again and push em through (there might be updates to the resources, too!)
            if reference in self.medication_ids:
                return None
            self.medication_ids.add(reference)

        try:
            medication = await fhir.download_reference(self.task_config.client, reference)
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning("Could not download Medication reference: %s", exc)
            self.summaries[1].had_errors = True

            if self.task_config.dir_errors:
                error_root = store.Root(os.path.join(self.task_config.dir_errors, self.name), create=True)
                error_path = error_root.joinpath("medication-fetch-errors.ndjson")
                with common.NdjsonWriter(error_path, "a") as writer:
                    writer.write(resource)

            return None

        return medication if self.scrub_medication(medication) else None

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        for resource in self.read_ndjson(progress=progress):
            orig_resource = copy.deepcopy(resource)
            if not self.scrubber.scrub_resource(resource):
                continue

            medication = await self.fetch_medication(orig_resource)
            yield resource, medication


class ObservationTask(tasks.EtlTask):
    name = "observation"
    resource = "Observation"
    tags = {"cpu"}


class PatientTask(tasks.EtlTask):
    name = "patient"
    resource = "Patient"
    tags = {"cpu"}


class ProcedureTask(tasks.EtlTask):
    name = "procedure"
    resource = "Procedure"
    tags = {"cpu"}


class ServiceRequestTask(tasks.EtlTask):
    name = "servicerequest"
    resource = "ServiceRequest"
    tags = {"cpu"}

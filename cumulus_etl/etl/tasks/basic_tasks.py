"""Standard FHIR resource tasks"""

import copy
import logging
import os
from typing import ClassVar

import pyarrow
import rich.progress

from cumulus_etl import common, completion, fhir, store
from cumulus_etl.etl import tasks


class AllergyIntoleranceTask(tasks.EtlTask):
    name: ClassVar = "allergyintolerance"
    resource: ClassVar = "AllergyIntolerance"
    tags: ClassVar = {"cpu"}


class ConditionTask(tasks.EtlTask):
    name: ClassVar = "condition"
    resource: ClassVar = "Condition"
    tags: ClassVar = {"cpu"}


class DeviceTask(tasks.EtlTask):
    name: ClassVar = "device"
    resource: ClassVar = "Device"
    tags: ClassVar = {"cpu"}


class DiagnosticReportTask(tasks.EtlTask):
    name: ClassVar = "diagnosticreport"
    resource: ClassVar = "DiagnosticReport"
    tags: ClassVar = {"cpu"}


class DocumentReferenceTask(tasks.EtlTask):
    name: ClassVar = "documentreference"
    resource: ClassVar = "DocumentReference"
    tags: ClassVar = {"cpu"}


class EncounterTask(tasks.EtlTask):
    """Processes Encounter FHIR resources"""

    name: ClassVar = "encounter"
    resource: ClassVar = "Encounter"
    tags: ClassVar = {"cpu"}

    # Encounters are a little more complicated than normal FHIR resources.
    # We also write out a table tying Encounters to a group name, for completion tracking.

    outputs: ClassVar = [
        # Write completion data out first, so that if an encounter is being completion-tracked,
        # there's never a gap where it doesn't have an entry. This will help downstream users
        # know if an Encounter is tracked or not - by simply looking at this table.
        tasks.OutputTable(**completion.completion_encounters_output_args()),
        tasks.OutputTable(),
    ]

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        async for encounter in super().read_entries(progress=progress):
            completion_info = {
                "encounter_id": encounter["id"],
                "group_name": self.task_config.export_group_name,
                "export_time": self.task_config.export_datetime.isoformat(),
            }
            yield completion_info, encounter

    @classmethod
    def get_schema(cls, resource_type: str | None, rows: list[dict]) -> pyarrow.Schema | None:
        if resource_type:
            return super().get_schema(resource_type, rows)
        else:
            return completion.completion_encounters_schema()


class ImmunizationTask(tasks.EtlTask):
    name: ClassVar = "immunization"
    resource: ClassVar = "Immunization"
    tags: ClassVar = {"cpu"}


class MedicationRequestTask(tasks.EtlTask):
    """Write MedicationRequest resources and associated Medication resources"""

    name: ClassVar = "medicationrequest"
    resource: ClassVar = "MedicationRequest"
    tags: ClassVar = {"cpu"}

    # We may write to a second Medication table as we go.
    # MedicationRequest can have inline medications via CodeableConcepts, or external Medication
    # references.
    # If external, we'll download them and stuff them in this output table.
    # We do all this special business logic because Medication is a special, "reference" resource,
    # and many EHRs don't let you simply bulk export them.

    outputs: ClassVar = [
        # Write medication out first, to avoid a moment where links are broken
        tasks.OutputTable(name="medication", resource_type="Medication"),
        tasks.OutputTable(),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Keep a cache of medication IDs that we've already downloaded.
        # If this ends up growing too large in practice, we can wipe it during a call to
        # table_batch_cleanup().
        # But let's try initially with keeping it around for the whole task.
        self.medication_ids = set()

        # Track whether we already warned about downloading Medications, to avoid spamming.
        self.warned_connection_error = False

    def scrub_medication(self, medication: dict | None) -> bool:
        """Scrub incoming medication resources, returns False if it should be skipped"""
        if not medication or not self.scrubber.scrub_resource(medication):  # standard scrubbing
            return False

        # Normally the above is all we'd need to do.
        # But this resource just came hot from the FHIR server, and we did not run the MS
        # anonymizer on it.
        # Since Medications are not patient-specific, we don't need the full MS treatment.
        # But still, we should probably drop some bits that might more easily identify the
        # *institution*.
        # This is a poor-man's MS config tool (and a blocklist rather than allow-list, but it's a
        # very simple resource)

        # *should* remove extensions at all layers, but this will catch 99% of them
        medication.pop("extension", None)
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
            # This will still duplicate medications from previous runs, but avoiding that feels
            # like more work than it's worth - just download em again and push em through (there
            # might be updates to the resources, too!)
            if reference in self.medication_ids:
                return None
            self.medication_ids.add(reference)

        try:
            medication = await fhir.download_reference(self.task_config.client, reference)
        except Exception as exc:
            if not self.warned_connection_error:
                logging.warning("Could not download Medication reference: %s", exc)
                self.warned_connection_error = True

            self.summaries[1].had_errors = True

            if self.task_config.dir_errors:
                error_root = store.Root(
                    os.path.join(self.task_config.dir_errors, self.name), create=True
                )
                error_path = error_root.joinpath("medication-fetch-errors.ndjson")
                with common.NdjsonWriter(error_path, append=True) as writer:
                    writer.write(resource)

            return None

        return medication if self.scrub_medication(medication) else None

    async def read_entries(self, *, progress: rich.progress.Progress = None) -> tasks.EntryIterator:
        # Load in any local Medication resources first. This lets the user prepare the linked
        # Medications ahead of time and feed them in alongside the MedicationRequests.
        # We'll note the IDs and avoid downloading them later when we do the MedicationRequests.
        resources = ["Medication", self.resource]
        for resource in self.read_ndjson(progress=progress, resources=resources):
            orig_resource = copy.deepcopy(resource)
            if not self.scrubber.scrub_resource(resource):
                continue

            if resource["resourceType"] == "Medication":
                self.medication_ids.add(f"Medication/{orig_resource['id']}")
                yield resource, None
            else:
                medication = await self.fetch_medication(orig_resource)
                yield medication, resource


class ObservationTask(tasks.EtlTask):
    name: ClassVar = "observation"
    resource: ClassVar = "Observation"
    tags: ClassVar = {"cpu"}


class PatientTask(tasks.EtlTask):
    name: ClassVar = "patient"
    resource: ClassVar = "Patient"
    tags: ClassVar = {"cpu"}


class ProcedureTask(tasks.EtlTask):
    name: ClassVar = "procedure"
    resource: ClassVar = "Procedure"
    tags: ClassVar = {"cpu"}


class ServiceRequestTask(tasks.EtlTask):
    name: ClassVar = "servicerequest"
    resource: ClassVar = "ServiceRequest"
    tags: ClassVar = {"cpu"}

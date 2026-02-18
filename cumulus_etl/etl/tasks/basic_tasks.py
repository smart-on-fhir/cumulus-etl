"""Standard FHIR resource tasks"""

from typing import ClassVar

import pyarrow

from cumulus_etl import completion, feedback
from cumulus_etl.etl import tasks


class AllergyIntoleranceTask(tasks.EtlTask):
    name: ClassVar = "allergyintolerance"
    resource: ClassVar = "AllergyIntolerance"


class ConditionTask(tasks.EtlTask):
    name: ClassVar = "condition"
    resource: ClassVar = "Condition"


class DeviceTask(tasks.EtlTask):
    name: ClassVar = "device"
    resource: ClassVar = "Device"


class DiagnosticReportTask(tasks.EtlTask):
    name: ClassVar = "diagnosticreport"
    resource: ClassVar = "DiagnosticReport"


class DocumentReferenceTask(tasks.EtlTask):
    name: ClassVar = "documentreference"
    resource: ClassVar = "DocumentReference"


class EncounterTask(tasks.EtlTask):
    """Processes Encounter FHIR resources"""

    name: ClassVar = "encounter"
    resource: ClassVar = "Encounter"

    # Encounters are a little more complicated than normal FHIR resources.
    # We also write out a table tying Encounters to a group name, for completion tracking.

    outputs: ClassVar = [
        # Write completion data out first, so that if an encounter is being completion-tracked,
        # there's never a gap where it doesn't have an entry. This will help downstream users
        # know if an Encounter is tracked or not - by simply looking at this table.
        tasks.OutputTable(**completion.completion_encounters_output_args()),
        tasks.OutputTable(),
    ]

    async def read_entries(self, *, progress: feedback.Progress = None) -> tasks.EntryIterator:
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


class LocationTask(tasks.EtlTask):
    name = "location"
    resource = "Location"


class MedicationTask(tasks.EtlTask):
    name = "medication"
    resource = "Medication"


class MedicationRequestTask(tasks.EtlTask):
    name = "medicationrequest"
    resource = "MedicationRequest"


class ObservationTask(tasks.EtlTask):
    name: ClassVar = "observation"
    resource: ClassVar = "Observation"


class OrganizationTask(tasks.EtlTask):
    name = "organization"
    resource = "Organization"


class PatientTask(tasks.EtlTask):
    name: ClassVar = "patient"
    resource: ClassVar = "Patient"


class PractitionerTask(tasks.EtlTask):
    name = "practitioner"
    resource = "Practitioner"


class PractitionerRoleTask(tasks.EtlTask):
    name = "practitionerrole"
    resource = "PractitionerRole"


class ProcedureTask(tasks.EtlTask):
    name: ClassVar = "procedure"
    resource: ClassVar = "Procedure"


class ServiceRequestTask(tasks.EtlTask):
    name: ClassVar = "servicerequest"
    resource: ClassVar = "ServiceRequest"

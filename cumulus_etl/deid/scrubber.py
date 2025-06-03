"""Cleans (de-identifies) a single resource node recursively"""

import logging
import tempfile
from typing import Any

import rich
import rich.padding
import rich.tree

from cumulus_etl import common, fhir
from cumulus_etl.deid import codebook, mstool, philter

# Record of unknown extensions (resource type -> extension URL -> count)
ExtensionCount = dict[str, dict[str, int]]


class SkipResource(Exception):
    pass


class SkipValue(Exception):
    pass


class MaskValue(SkipValue):
    pass


class Scrubber:
    """
    Manages de-identification for FHIR resources.

    This usually involves holding a mapping of true IDs to fake IDs and removing identifying attributes from resources.

    It's meant to persist throughout the whole de-identification process (to keep the ID mappings around, across
    resources).

    Scrubbing happens in two phases:
    1. Bulk de-identification of all the input data early on. This is a first pass for almost everything (except IDs,
       notes, and sundries that Cumulus actually wants to see). Dates of birth are generalized, for example, and lots
       of PHI attributes like names or contact info is redacted. After this call, the data is partially de-identified.
    2. Per-resource de-identification as Cumulus inspects each resource. This is to give Cumulus more control over the
       bits of PHI data that it actually cares about (like IDs or notes) before this final scrub here. After this call,
       the resource is fully de-identified.
    """

    def __init__(self, codebook_dir: str | None = None, use_philter: bool = False):
        self.codebook = codebook.Codebook(codebook_dir)
        self.philter = philter.Philter() if use_philter else None
        # List of ignored extensions (resource -> url -> count)
        self.dropped_extensions: ExtensionCount = {}
        # List of modifier extensions that caused us to skip a resource (resource -> url -> count)
        self.skipped_modifer_extensions: ExtensionCount = {}

    @staticmethod
    async def scrub_bulk_data(input_dir: str) -> tempfile.TemporaryDirectory:
        """
        Bulk de-identification of all input data

        This assumes that input_dir holds several FHIR ndjson files.

        :returns: a temporary directory holding the de-identified results, in FHIR ndjson format
        """
        tmpdir = tempfile.TemporaryDirectory()
        await mstool.run_mstool(input_dir, tmpdir.name)
        return tmpdir

    def scrub_resource(
        self, node: dict, scrub_attachments: bool = True, keep_stats: bool = True
    ) -> bool:
        """
        Cleans/de-identifies resource (in-place) and returns False if it should be rejected

        Some resources should be ignored (because we don't understand its modifier extensions).
        When that happens, this returns False.

        :param node: resource to de-identify
        :param scrub_attachments: whether to remove any attachment data found
        :param keep_stats: whether to records stats about this resource
        :returns: whether this resource is allowed to be used
        """
        try:
            self._scrub_node(
                node.get("resourceType"),
                "root",
                node,
                scrub_attachments=scrub_attachments,
                keep_stats=keep_stats,
            )
        except SkipValue:
            return False  # an empty resource? Not even an "id"? Skip invalid entry
        except SkipResource:
            # No need to log, we'll have already reported out what we should
            return False
        except ValueError as exc:
            logging.warning("Could not parse value: %s", exc)
            return False

        return True

    def scrub_text(self, text: str) -> str:
        """
        Scrub text of any detected PHI.

        :param text: the text to scrub
        :returns: the scrubbed text, with PHI replaced by asterisks ("*")
        """
        return self.philter.scrub_text(text) if self.philter else text

    def save(self) -> None:
        """Saves any resources used to persist across runs (like the codebook)"""
        self.codebook.save()

    def print_extension_report(self) -> None:
        self._print_extension_table(
            "Unrecognized extensions dropped from resources:",
            self.dropped_extensions,
        )
        self._print_extension_table(
            "🚨 Resources skipped due to unrecognized modifier extensions: 🚨",
            self.skipped_modifer_extensions,
        )

    ###############################################################################
    #
    # Implementation details
    #
    ###############################################################################

    def _scrub_node(
        self,
        resource_type: str,
        node_path: str,
        node: dict,
        *,
        scrub_attachments: bool,
        keep_stats: bool,
        inside_extension: bool = False,
    ) -> None:
        """Examines all properties of a node and modifies node in-place"""
        for key, values in list(node.items()):
            if values is None:
                continue

            is_list = isinstance(values, list)
            if not is_list:
                # Make everything a list for ease of the next bit where we iterate a list
                values = [values]

            result = []
            for value in values:
                try:
                    result.append(
                        self._scrub_single_value(
                            resource_type,
                            node_path,
                            key,
                            value,
                            scrub_attachments=scrub_attachments,
                            keep_stats=keep_stats,
                            inside_extension=inside_extension,
                        )
                    )
                except MaskValue:
                    # TODO: (not needed yet) support masking values inside array fields
                    parent = node.setdefault(f"_{key}", {})
                    self._add_data_absent_extension(parent)
                except SkipValue:
                    pass

            # Write the result array back to node (safe because we did list(node.items()) above)
            if result:
                node[key] = result if is_list else result[0]
            else:
                del node[key]  # must have gotten SkipValue, erase the key for cleanliness

        # Skip empty structs for cleanliness
        if not node:
            raise SkipValue

    def _scrub_single_value(
        self,
        resource_type: str,
        node_path: str,
        key: str,
        value: Any,
        *,
        scrub_attachments: bool,
        keep_stats: bool,
        inside_extension: bool = False,
    ) -> Any:
        """Examines one single property of a node"""
        # For now, just manually run each operation. If this grows further, we can abstract it more.
        value = self._check_ids(resource_type, key, value)
        value = self._check_security(node_path, key, value)
        value = self._check_system(key, value)
        value = self._check_text(key, value)
        if scrub_attachments:
            value = self._check_attachments(resource_type, node_path, key, value)

        # If this is an extension, see if we should ignore it / ignore the resource
        if not inside_extension:  # don't examine extensions-in-extensions
            if self._check_extensions(resource_type, key, value, keep_stats=keep_stats):
                inside_extension = True
            if self._check_modifier_extensions(resource_type, key, value, keep_stats=keep_stats):
                inside_extension = True

        # Recurse if we are holding another FHIR object (i.e. a dict instead of a string)
        if isinstance(value, dict):
            self._scrub_node(
                resource_type,
                f"{node_path}.{key}",
                value,
                scrub_attachments=scrub_attachments,
                keep_stats=keep_stats,
                inside_extension=inside_extension,
            )

        return value

    def _print_extension_table(self, title: str, table: ExtensionCount) -> None:
        if not table:
            return  # nothing to do!

        common.print_header(title)
        for resource_type in sorted(table):
            tree = rich.tree.Tree(resource_type)
            for url, count in sorted(table[resource_type].items()):
                tree.add(f"{url} ({count:,} time{'' if count == 1 else 's'})")
            indented = rich.padding.Padding.indent(tree, 1)
            rich.get_console().print(indented)

    def _add_data_absent_extension(self, parent: dict) -> None:
        extensions = parent.setdefault("extension", [])

        # Check if the value is already marked as absent for any reason - leave it in place.
        # (though that would be weird, since the field was present or we wouldn't be in this path)
        for extension in extensions:
            if extension.get("url") == "http://hl7.org/fhir/StructureDefinition/data-absent-reason":
                return

        # See https://hl7.org/fhir/extensions/StructureDefinition-data-absent-reason.html
        extensions.append(
            {
                "url": "http://hl7.org/fhir/StructureDefinition/data-absent-reason",
                "valueCode": "masked",
            }
        )

    ###############################################################################
    #
    # Individual checkers
    #
    ###############################################################################

    def _count_unknown_extension(self, table: ExtensionCount, resource: str, url: str) -> None:
        resource_counts = table.setdefault(resource, {})
        count = resource_counts.setdefault(url, 0)
        resource_counts[url] = count + 1

    def _check_extensions(
        self, resource_type: str, key: str, value: Any, *, keep_stats: bool
    ) -> None:
        """If there's any unrecognized extensions, log and delete them"""
        if key == "extension" and isinstance(value, dict):
            # We want to allow almost any extension that might have clinical or QA relevance.
            # But we use an allow list to avoid letting in PHI.
            allowed_extensions = {
                ### Base spec extensions
                ### (See https://www.hl7.org/fhir/R4/extensibility-registry.html)
                "http://hl7.org/fhir/StructureDefinition/condition-assertedDate",
                "http://hl7.org/fhir/StructureDefinition/data-absent-reason",
                "http://hl7.org/fhir/StructureDefinition/derivation-reference",
                "http://hl7.org/fhir/StructureDefinition/event-performerFunction",
                "http://hl7.org/fhir/StructureDefinition/individual-pronouns",
                "http://hl7.org/fhir/StructureDefinition/iso21090-PQ-translation",
                "http://hl7.org/fhir/StructureDefinition/patient-genderIdentity",
                "http://hl7.org/fhir/StructureDefinition/workflow-supportingInfo",
                "http://hl7.org/fhir/5.0/StructureDefinition/extension-DocumentReference.attester",
                ### US Core extensions
                ### (See https://hl7.org/fhir/us/core/profiles-and-extensions.html)
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-genderIdentity",
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-jurisdiction",
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medication-adherence",
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-sex",
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-tribal-affiliation",
                ### Next two are old US Core DSTU1 URLs, still seen in the wild
                ### (See https://www.hl7.org/fhir/DSTU1/us-core.html)
                "http://hl7.org/fhir/Profile/us-core#ethnicity",
                "http://hl7.org/fhir/Profile/us-core#race",
                ### Cerner extensions
                # Links to a client Organization reference in an Encounter
                "https://fhir-ehr.cerner.com/r4/StructureDefinition/client-organization",
                # "precision" here is the precision of a date field
                "https://fhir-ehr.cerner.com/r4/StructureDefinition/precision",
                # Some medication extensions used by Cerner
                "https://fhir-ehr.cerner.com/r4/StructureDefinition/pharmacy-verification-status",
                "http://electronichealth.se/fhir/StructureDefinition/NLLDosePackaging",
                "http://electronichealth.se/fhir/StructureDefinition/NLLPrescriptionFormat",
                "http://electronichealth.se/fhir/StructureDefinition/NLLRegistrationBasis",
                ### Epic extensions
                "http://open.epic.com/FHIR/StructureDefinition/extension/accidentrelated",
                "http://open.epic.com/FHIR/StructureDefinition/extension/calculated-pronouns-to-use-for-text",
                "http://open.epic.com/FHIR/StructureDefinition/extension/clinical-note-attached-media",
                "http://open.epic.com/FHIR/StructureDefinition/extension/clinical-note-authentication-instant",
                "http://open.epic.com/FHIR/StructureDefinition/extension/clinical-note-author-provider-type",
                "http://open.epic.com/FHIR/StructureDefinition/extension/clinical-note-interval-update",
                "http://open.epic.com/FHIR/StructureDefinition/extension/clinical-note-interval-update-source",
                "http://open.epic.com/FHIR/StructureDefinition/extension/clinical-note-post-procedure-diagnosis",
                "http://open.epic.com/FHIR/StructureDefinition/extension/clinical-note-pre-procedure-diagnosis",
                "http://open.epic.com/FHIR/StructureDefinition/extension/clinical-note-service",
                "http://open.epic.com/FHIR/StructureDefinition/extension/edd-at-begin-exam",
                "http://open.epic.com/FHIR/StructureDefinition/extension/ip-admit-datetime",
                "http://open.epic.com/FHIR/StructureDefinition/extension/legal-sex",
                "http://open.epic.com/FHIR/StructureDefinition/extension/log-level-procedure-codes",
                "http://open.epic.com/FHIR/StructureDefinition/extension/observation-datetime",
                "http://open.epic.com/FHIR/StructureDefinition/extension/patient-type",
                "http://open.epic.com/FHIR/StructureDefinition/extension/sex-for-clinical-use",
                "http://open.epic.com/FHIR/StructureDefinition/extension/smartdata",
                "http://open.epic.com/FHIR/StructureDefinition/extension/specialty",
                "http://open.epic.com/FHIR/StructureDefinition/extension/surgical-history-laterality",
                "http://open.epic.com/FHIR/StructureDefinition/extension/surgical-history-source",
                "http://open.epic.com/FHIR/StructureDefinition/extension/template-id",
                "http://open.epic.com/FHIR/STU3/StructureDefinition/patient-preferred-provider-language",
                "http://open.epic.com/FHIR/STU3/StructureDefinition/patient-preferred-provider-sex",
                "http://open.epic.com/FHIR/STU3/StructureDefinition/temperature-in-fahrenheit",
                "http://open.epic.com/FHIR/R4/StructureDefinition/patient-preferred-provider-sex",
                "https://open.epic.com/FHIR/StructureDefinition/patient-merge-target-reference",
                "https://open.epic.com/FHIR/StructureDefinition/patient-merge-unmerge-instant",
                # A Netherlands extension used by Epic
                "http://nictiz.nl/fhir/StructureDefinition/BodySite-Qualifier",
                ### Synthea extensions
                "http://synthetichealth.github.io/synthea/disability-adjusted-life-years",
                "http://synthetichealth.github.io/synthea/quality-adjusted-life-years",
            }
            # Some extensions we know about, but aren't necessary to us (they duplicate standard
            # extensions, contain PHI, or are otherwise not relevant). We don't want to warn
            # the user about them as "unrecognized", so we just ignore them entirely.
            ignored_extensions = {
                # Base spec extensions related to PHI
                "http://hl7.org/fhir/StructureDefinition/geolocation",
                "http://hl7.org/fhir/StructureDefinition/iso21090-EN-qualifier",
                "http://hl7.org/fhir/StructureDefinition/iso21090-TEL-address",
                "http://hl7.org/fhir/StructureDefinition/patient-birthPlace",
                "http://hl7.org/fhir/StructureDefinition/patient-birthTime",
                "http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName",
                # Usually harmless, but we ignore it to avoid accidentally leaving in the
                # rendered value of a PHI element that we removed or didn't allow-list.
                "http://hl7.org/fhir/StructureDefinition/rendered-value",
                # US Core extension, but deals with how to email a person, which we don't need
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-direct",
                # Cerner dosage instruction freeform text extension, could have PHI
                "https://fhir-ehr.cerner.com/r4/StructureDefinition/clinical-instruction",
                # Cerner arbitrary key/value extension - could have PHI?
                "https://fhir-ehr.cerner.com/r4/StructureDefinition/custom-attribute",
                # Cerner financial extension for Encounters, we don't need to record this
                "https://fhir-ehr.cerner.com/r4/StructureDefinition/estimated-financial-responsibility-amount",
                # Epic PHI extension
                "http://open.epic.com/FHIR/StructureDefinition/extension/birth-location",
                # Epic extension that points at an Encounter Identifier, the kind we normally strip
                "http://open.epic.com/FHIR/StructureDefinition/extension/ce-encounter-id",
                # Epic extension with physician name
                "http://open.epic.com/FHIR/StructureDefinition/extension/lab-e-signature",
                # Epic extension with internal team names, low clinical relevance but w/ PHI risk
                "http://open.epic.com/FHIR/StructureDefinition/extension/team-name",
            }
            url = value.get("url")
            if url not in allowed_extensions:
                if keep_stats and url not in ignored_extensions:
                    self._count_unknown_extension(self.dropped_extensions, resource_type, url)
                raise SkipValue
            return True
        return False

    def _check_modifier_extensions(
        self, resource_type: str, key: str, value: Any, *, keep_stats: bool
    ) -> None:
        """If there's any unrecognized modifierExtensions, raise a SkipResource exception"""
        if key == "modifierExtension" and isinstance(value, dict):
            allowed_extensions = {
                # These NLP extensions are generated by ctakesclient's text2fhir code.
                # While we don't anticipate ingesting any resources using these extensions
                # (and we don't currently generate them ourselves), we might in the future.
                "http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity",
                "http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-source",
            }
            url = value.get("url")
            if url not in allowed_extensions:
                if keep_stats:
                    self._count_unknown_extension(
                        self.skipped_modifer_extensions, resource_type, url
                    )
                raise SkipResource
            return True
        return False

    def _check_ids(self, resource_type: str, key: str, value: Any) -> Any:
        """Converts any IDs and references to a de-identified version"""
        # ID values ("id" is only ever used as a resource/element ID)
        # Can occur at the root node, contained resources, and TECHNICALLY in any backbone element.
        # But the backbone elements won't have resourceType, and we can ignore those as non-resources/non-PHI.
        # So we only convert if there's also a resourceType present.
        if key == "id":
            value = self.codebook.fake_id(resource_type, value)

        # References
        # "reference" can sometimes be a URL or non-FHIRReference element -- at some point we'll need to be smarter.
        elif isinstance(value, dict) and "reference" in value:
            ref_type, real_id = fhir.unref_resource(value)

            # Handle contained references (see comments in unref_resource for more detail)
            prefix = ""
            if real_id.startswith("#"):
                prefix = "#"
                real_id = real_id[1:]

            fake_id = f"{prefix}{self.codebook.fake_id(ref_type, real_id)}"
            value["reference"] = fhir.ref_resource(ref_type, fake_id)["reference"]

        return value

    def _check_text(self, key: str, value: Any) -> Any:
        """Scrubs text values that got through the MS config by passing them through philter"""
        # Mostly, we are filtering any freeform text fields we previously allowed in with ms-config.json.
        if isinstance(value, str) and any(
            (
                # Coding.display is clinically unnecessary but is useful for rendering.
                # Since "code" is always present, downstream consumers can & should provide their
                # own display label. But we don't remove it entirely, for cases where unexpected
                # codes are used. Note that this will definitely over-scrub (like scrubbing "White"
                # from the USCDI race extension), but again -- this display value is redundant and
                # rather than try to be smart, we're safely dumb.
                key == "display",
                # CodeableConcept.text has clinical value for situations that don't have clear
                # coding yet, like early-days Covid day PCRs. And text-only codeable concepts show
                # up a lot when the EHR allows it. Hence why we normally let it through.
                # But we should still scrub it since it is loose text that could hold PHI.
                key == "text",
            )
        ):
            value = self.scrub_text(value)

        if isinstance(value, str) and any(
            (
                # Observation.valueString has some clinical value, but often holds PHI.
                # If we ever want to use this for clinical purposes, we should process it via NLP
                # on the ETL side (like we do for DocumentReference attachments).
                key == "valueString",
            )
        ):
            raise MaskValue

        return value

    @staticmethod
    def _check_attachments(resource_type: str, node_path: str, key: str, value: Any) -> Any:
        """Strip any attachment data"""
        if any(
            (
                (resource_type == "DiagnosticReport" and node_path == "root.presentedForm"),
                (resource_type == "DocumentReference" and node_path == "root.content.attachment"),
            )
        ):
            if key in {"data", "url"}:
                raise MaskValue

        return value

    @staticmethod
    def _check_security(node_path: str, key: str, value: Any) -> Any:
        """
        Strip any security data that the MS tool injects

        It takes up space in the result and anyone using Cumulus ETL understands that there was ETL applied.
        """
        if node_path == "root.meta" and key == "security":
            # maybe too aggressive -- is there data we care about in meta.security?
            raise SkipValue

        return value

    def _check_system(self, key: str, value: Any) -> Any:
        """
        Strips any code/display fields that might be sensitive under some systems
        """
        # System is used in Quantity, Coding, ContactPoint, and Identifier.
        # We strip ContactPoint and Identifiers during the ms-config.json step.
        # Quantity has a 'code' free text property and Coding has both 'code' and 'display'.
        if isinstance(value, dict) and "system" in value:
            system = value["system"]
            code = value.get("code")
            display = value.get("display")
            code_ok, display_ok = self._is_system_allowed(system, code, display)
            if not code_ok or not display_ok:
                if not code_ok:
                    value.pop("code", None)
                if not display_ok:
                    value.pop("display", None)
                self._add_data_absent_extension(value)

        return value

    @staticmethod
    def _is_system_allowed(system: str, code: str | None, display: str | None) -> tuple[bool, bool]:
        """
        Checks whether the code and/or display are permissible for the given system.

        This method is primarily to check on an Epic customer extension point system.
        We've seen PHI in here, in both the code and display value. This happens because Epic
        custom systems seem to start with a base valueset and then allows site-customizations on
        top of that. And those customizations are free-form. So we've seen names and other PHI
        details in there for one-off free-form entries by data entry folks.
        However, Epic uses these systems in several important places, with no standard code
        system alongside - notably Encounter.class. So we try to detect signs of custom code
        values and mask them.
        """

        # Allow any system that isn't inside the Epic extension point system
        if not system.startswith("urn:oid:1.2.840.114350."):
            return True, True

        if not code:  # strip display if it's defined, but no code is present
            return True, not display

        # For zero codes ("other"), we've seen custom display values.
        if code == "0" and display:
            return True, False

        # Epic uses numeric codes for its internal code systems.
        # Non-numeric codes are custom.
        try:
            int(code)
        except ValueError:
            return False, False

        return True, True

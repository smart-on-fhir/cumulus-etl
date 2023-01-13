"""Cleans (de-identifies) a single resource node recursively"""

import collections
import logging
import tempfile
from typing import Any

from fhirclient.models.attachment import Attachment
from fhirclient.models.fhirabstractbase import FHIRAbstractBase
from fhirclient.models.fhirabstractresource import FHIRAbstractResource
from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.meta import Meta

from cumulus import fhir_common
from cumulus.deid import codebook, mstool


FHIRProperty = collections.namedtuple(
    "FHIRProperty",
    ["name", "json_name", "type", "is_list", "of_many", "not_optional"],
)


class SkipResource(Exception):
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

    def __init__(self, codebook_dir: str = None):
        self.codebook = codebook.Codebook(codebook_dir)
        self.codebook_dir = codebook_dir

    @staticmethod
    def scrub_bulk_data(input_dir: str) -> tempfile.TemporaryDirectory:
        """
        Bulk de-identification of all input data

        This assumes that input_dir holds several FHIR ndjson files.

        :returns: a temporary directory holding the de-identified results, in FHIR ndjson format
        """
        tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        mstool.run_mstool(input_dir, tmpdir.name)
        return tmpdir

    def scrub_resource(self, node: FHIRAbstractBase, scrub_attachments: bool = True) -> bool:
        """
        Cleans/de-identifies resource (in-place) and returns False if it should be rejected

        Some resources should be ignored (because we don't understand its modifier extensions).
        When that happens, this returns False.

        :param node: resource to de-identify
        :param scrub_attachments: whether to remove any attachment data found
        :returns: whether this resource is allowed to be used
        """
        try:
            self._scrub_node(node, scrub_attachments=scrub_attachments)
        except SkipResource as exc:
            logging.warning("Ignoring resource of type %s: %s", node.__class__.__name__, exc)
            return False
        except ValueError as exc:
            logging.warning("Could not parse value: %s", exc)
            return False

        return True

    def save(self) -> None:
        """Saves any resources used to persist across runs (like the codebook)"""
        if self.codebook_dir:
            self.codebook.db.save(self.codebook_dir)

    ###############################################################################
    #
    # Implementation details
    #
    ###############################################################################

    def _scrub_node(self, node: FHIRAbstractBase, scrub_attachments: bool) -> None:
        """Examines all properties of a node"""
        fhir_properties = node.elementProperties()
        for fhir_property in map(FHIRProperty._make, fhir_properties):
            values = getattr(node, fhir_property.name)
            if values is None:
                continue

            if not fhir_property.is_list:
                # Make everything a list for ease of the next bit where we iterate a list
                values = [values]

            for value in values:
                self._scrub_single_value(node, fhir_property, value, scrub_attachments=scrub_attachments)

    def _scrub_single_value(
        self, node: FHIRAbstractBase, fhir_property: FHIRProperty, value: Any, scrub_attachments: bool
    ) -> None:
        """Examines one single property of a node"""
        # For now, just manually run each operation. If this grows further, we can abstract it more.
        self._check_ids(node, fhir_property, value)
        self._check_modifier_extensions(fhir_property, value)
        self._check_security(node, fhir_property, value)
        if scrub_attachments:
            self._check_attachments(node, fhir_property)

        # Recurse if we are holding a real FHIR object (instead of, say, a string)
        if isinstance(value, FHIRAbstractBase):
            self._scrub_node(value, scrub_attachments=scrub_attachments)

    ###############################################################################
    #
    # Individual checkers
    #
    ###############################################################################

    @staticmethod
    def _check_modifier_extensions(fhir_property: FHIRProperty, value: Any) -> None:
        """If there's any unrecognized modifierExtensions, raise a SkipResource exception"""
        if fhir_property.name == "modifierExtension":
            known_extensions = [
                "http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity",
                "http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-source",
            ]
            url = getattr(value, "url", None)
            if url not in known_extensions:
                raise SkipResource(f'Unrecognized modifierExtension with URL "{url}"')

    def _check_ids(self, node: FHIRAbstractBase, fhir_property: FHIRProperty, value: Any) -> None:
        """Converts any IDs and references to a de-identified version"""
        # ID values
        if isinstance(node, FHIRAbstractResource) and fhir_property.name == "id":
            node.id = self.codebook.fake_id(node.resource_type, value)

        # References
        elif isinstance(node, FHIRReference) and fhir_property.name == "reference":
            resource_type, real_id = fhir_common.unref_resource(node)
            fake_id = self.codebook.fake_id(resource_type, real_id)
            node.reference = fhir_common.ref_resource(resource_type, fake_id).reference

    @staticmethod
    def _check_attachments(node: FHIRAbstractBase, fhir_property: FHIRProperty) -> None:
        """Strip any attachment data"""
        if isinstance(node, Attachment) and fhir_property.name == "data":
            node.data = None

    @staticmethod
    def _check_security(node: FHIRAbstractBase, fhir_property: FHIRProperty, value: Any) -> None:
        """
        Strip any security data that the MS tool injects

        It takes up space in the result and anyone using Cumulus ETL understands that there was ETL applied.
        """
        if fhir_property.name == "meta" and isinstance(value, Meta):
            value.security = None  # maybe too aggressive -- is there data we care about in meta.security?

            # If we wiped out the only content in Meta, remove it so as not to confuse downstream bits like parquet
            # writers which try to infer values from an empty struct and fail.
            if value.as_json() == {}:
                node.meta = None

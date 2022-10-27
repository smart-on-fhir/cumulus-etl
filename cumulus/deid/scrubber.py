"""Cleans (de-identifies) a single resource node recursively"""

import collections
import logging
from typing import Any

from fhirclient.models.attachment import Attachment
from fhirclient.models.fhirabstractbase import FHIRAbstractBase
from fhirclient.models.fhirabstractresource import FHIRAbstractResource
from fhirclient.models.fhirreference import FHIRReference

from cumulus import fhir_common
from cumulus.deid import codebook


FHIRProperty = collections.namedtuple(
    'FHIRProperty',
    ['name', 'json_name', 'type', 'is_list', 'of_many', 'not_optional'],
)


class SkipResource(Exception):
    pass


class Scrubber:
    """
    Manages de-identification for FHIR resources.

    This usually involves holding a mapping of true IDs to fake IDs and removing identifying attributes from resources.

    It's meant to persist throughout the whole de-identification process (to keep the ID mappings around, across
    resources).
    """
    def __init__(self, codebook_path: str = None):
        self.codebook = codebook.Codebook(codebook_path)
        self.codebook_path = codebook_path

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
            logging.warning('Ignoring resource of type %s: %s', node.__class__.__name__, exc)
            return False
        except ValueError as exc:
            logging.warning('Could not parse value: %s', exc)
            return False

        return True

    def save(self) -> None:
        """Saves any resources used to persist across runs (like the codebook)"""
        if self.codebook_path:
            self.codebook.db.save(self.codebook_path)

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

    def _scrub_single_value(self, node: FHIRAbstractBase, fhir_property: FHIRProperty, value: Any,
                            scrub_attachments: bool) -> None:
        """Examines one single property of a node"""
        # For now, just manually run each operation. If this grows further, we can abstract it more.
        self._check_ids(node, fhir_property, value)
        self._check_modifier_extensions(fhir_property, value)
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
        if fhir_property.name == 'modifierExtension':
            known_extensions = [
                'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity',
                'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-source',
            ]
            url = getattr(value, 'url', None)
            if url not in known_extensions:
                raise SkipResource(f'Unrecognized modifierExtension with URL "{url}"')

    def _check_ids(self, node: FHIRAbstractBase, fhir_property: FHIRProperty, value: Any) -> None:
        """Converts any IDs and references to a de-identified version"""
        # ID values
        if isinstance(node, FHIRAbstractResource) and fhir_property.name == 'id':
            node.id = self.codebook.fake_id(node.resource_type, value)

        # References
        elif isinstance(node, FHIRReference) and fhir_property.name == 'reference':
            resource_type, real_id = fhir_common.unref_resource(node)
            fake_id = self.codebook.fake_id(resource_type, real_id)
            node.reference = fhir_common.ref_resource(resource_type, fake_id).reference

    @staticmethod
    def _check_attachments(node: FHIRAbstractBase, fhir_property: FHIRProperty) -> None:
        """Strip any attachment data"""
        if isinstance(node, Attachment) and fhir_property.name == 'data':
            node.data = None

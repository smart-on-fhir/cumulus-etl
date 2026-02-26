"""Cleans (de-identifies) a single resource node recursively"""

import logging
import os
import tomllib
from typing import Any

import rich
import rich.padding
import rich.tree

from cumulus_etl import common, fhir
from cumulus_etl.deid import codebook, philter

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

    This usually involves holding a mapping of true IDs to fake IDs and removing identifying
    attributes from resources.

    It's meant to persist throughout the whole de-identification process (to keep the ID mappings
    around, across resources).

    Note that this doesn't do VALIDATION so much. FHIR rules are not checked. Data types might be
    wrong. Our overriding interest here is keeping PHI out. Validation will happen later when
    writing to the output target, and using detected schemas from cumulus-fhir-support.
    """

    def __init__(
        self,
        codebook_dir: str | None = None,
        use_philter: bool = False,
        mask_notes: bool = True,
        keep_stats: bool = True,
        cache_ids: bool = True,
    ):
        self.codebook = codebook.Codebook(codebook_dir, cache_ids=cache_ids)
        self.philter = philter.Philter() if use_philter else None
        self.mask_notes = mask_notes
        self.keep_stats = keep_stats
        # List of ignored extensions (resource -> url -> count)
        self.dropped_extensions: ExtensionCount = {}
        # List of modifier extensions that caused us to skip a resource (resource -> url -> count)
        self.skipped_modifer_extensions: ExtensionCount = {}

        config_path = os.path.join(os.path.dirname(__file__), "scrub-rules.toml")
        with open(config_path, "rb") as f:
            self.config = tomllib.load(f)

    def scrub_resource(self, node: dict) -> bool:
        """
        Cleans/de-identifies resource (in-place) and returns False if it should be rejected

        Some resources should be ignored (because we don't understand its modifier extensions).
        When that happens, this returns False.

        :param node: resource to de-identify
        :returns: whether this resource is allowed to be used
        """
        try:
            self._scrub_resource_node(node)
        except SkipValue:
            return False  # an empty resource? Not even an "id"? Skip invalid entry
        except SkipResource:
            # No need to log, we'll have already reported out what we should
            return False
        except ValueError as exc:
            logging.warning("Could not parse value: %s", exc)
            return False

        return True

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

    def _get_node_config(self, name: str) -> dict:
        node_config = {**self.config["allowed"][name]}
        while "_extends" in node_config:
            parent = node_config.pop("_extends")
            node_config.update(self.config["allowed"][parent])
        return node_config

    def _scrub_resource_node(self, node: dict) -> None:
        if not isinstance(node, dict):
            raise SkipValue

        resource_type = node.get("resourceType")
        if not resource_type:
            raise SkipValue

        if resource_type not in self.config["allowed"]:
            resource_type = "Resource"  # just use basic resource type then

        node_config = self._get_node_config(resource_type)
        self._scrub_node(resource_type, node_config, node)

    def _scrub_node(
        self,
        resource_type: str,
        node_config: dict,
        node: dict,
        *,
        conditions: dict | None = None,
        inside_extension: bool = False,
    ) -> None:
        """Examines all properties of a node and modifies node in-place"""
        if conditions:
            for key, vals in conditions.items():
                if node.get(key) not in vals:
                    raise SkipValue  # this node doesn't match our conditions! skip it

        for key, values in list(node.items()):
            if values is None:
                del node[key]
                continue

            if key.startswith("_"):
                # This is a primitive type extension node.
                # See http://hl7.org/fhir/R4/json.html#primitive
                if key[1:] in node_config:
                    key_config = self._get_node_config("Element")
                else:
                    key_config = None  # unrecognized key, let's skip it
            else:
                key_config = node_config.get(key)

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
                            key_config,
                            node,
                            key,
                            value,
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
        action: str,
        parent: dict,
        key: str,
        value: Any,
        *,
        inside_extension: bool = False,
    ) -> Any:
        """Examines one single property of a node"""
        if action is None:
            # no config for this key, so drop it on the floor
            raise SkipValue

        is_node = isinstance(value, dict)

        if action == "CC":
            action = "CodeableConcept"
        elif action == "Ref":
            action = "Reference"

        is_final_action = isinstance(action, str) and action[0].islower()

        if is_final_action == is_node:
            # This is a config error, not a user error - users should realistically never see this.
            # Unless their FHIR is way out of spec.
            logging.warning(
                f"Unexpected formatting for resource {resource_type}, action {action}, key {key}."
            )
            raise SkipResource  # who knows what's happening here

        if is_final_action:
            if action.startswith("check-system;"):
                if self._check_invalid_for_system(
                    key, parent.get("system"), parent.get("code"), parent.get("display")
                ):
                    self._add_data_absent_extension(parent)
                    raise SkipValue
                action = action.split(";", 1)[-1]

            if isinstance(value, str):
                # Strip whitespace from any string, to match the behavior of our old MS deid tool.
                # We do see this in real world data (trailing spaces on Epic `CodeableConcept.text`
                # fields for example).
                value = value.strip()
                if not value:  # drop empty strings (also matching old MS deid tool behavior)
                    raise SkipValue

            match action:
                case "val":
                    return value
                case "mask":
                    raise MaskValue
                case "mask-note":
                    if self.mask_notes:
                        raise MaskValue
                    return value
                case "anon-id":
                    return self._anon_id(resource_type, value)
                case "anon-ref":
                    return self._anon_ref(resource_type, parent)
                case "year":
                    return value[:4]
                case "zip":
                    return self._zip(value)
                case "philter":
                    return self._philter(value)
                case _:  # pragma: no cover
                    logging.warning(f"Unknown action {action}.")
                    raise SkipResource  # who knows what's happening here

        # We're still here - it _should_ be a node we can recurse on

        conditions = {}

        if isinstance(action, dict):
            # There is more config to process, we aren't handling the whole value at once.
            # e.g. with config: `reaction.onset = "val"` and we are on the "reaction" step.
            # Such structs are BackboneElements (see http://hl7.org/fhir/R4/backboneelement.html).
            next_config = self._get_node_config("BackboneElement")
            next_config.update(action)
        elif action == "Resource":  # Special case new toplevel resources (to override res type)
            self._scrub_resource_node(value)
            return value
        else:
            # Check for any optional conditions, specified via semicolon ("Identifier;system=url")
            pieces = action.split(";")
            for piece in pieces[1:]:
                con_key, con_value = piece.split("=", 1)
                conditions.setdefault(con_key, set()).add(con_value)
            next_config = self._get_node_config(pieces[0])

        # Special case extensions here, so we can count them / bail if we see an unrecognized
        # modifierExtension.
        if action == "Extension" and not inside_extension:
            if key == "modifierExtension":
                self._check_modifier_extensions(resource_type, key, value)
            else:
                self._check_extensions(resource_type, key, value)
            inside_extension = True

        self._scrub_node(
            resource_type,
            next_config,
            value,
            conditions=conditions,
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

    def _philter(self, text: str) -> str:
        return self.philter.scrub_text(text) if self.philter else text

    def _check_extensions(self, resource_type: str, key: str, value: Any) -> None:
        """If there's any unrecognized extensions, log and delete them"""
        config = self.config.get("extensions", {}).get("normal", {})
        allowed_extensions = config.get("allowed", [])
        ignored_extensions = config.get("ignored", [])
        url = value.get("url")
        if url not in allowed_extensions:
            if self.keep_stats and url not in ignored_extensions:
                self._count_unknown_extension(self.dropped_extensions, resource_type, url)
            raise SkipValue
        return True

    def _check_modifier_extensions(self, resource_type: str, key: str, value: Any) -> None:
        """If there's any unrecognized modifierExtensions, raise a SkipResource exception"""
        config = self.config.get("extensions", {}).get("modifier", {})
        allowed_extensions = config.get("allowed", [])
        url = value.get("url")
        if url not in allowed_extensions:
            if self.keep_stats:
                self._count_unknown_extension(self.skipped_modifer_extensions, resource_type, url)
            raise SkipResource
        return True

    def _anon_id(self, resource_type: str, value: str) -> str:
        return self.codebook.fake_id(resource_type, value)

    def _anon_ref(self, resource_type: str, value: str) -> str:
        # A "reference" can sometimes be a URL or non-FHIRReference element.
        # At some point we'll need to be smarter.
        ref_type, real_id = fhir.unref_resource(value)

        # Handle contained references (see comments in unref_resource for more detail)
        prefix = ""
        if real_id.startswith("#"):
            prefix = "#"
            real_id = real_id[1:]

        fake_id = f"{prefix}{self._anon_id(ref_type, real_id)}"
        return fhir.ref_resource(ref_type, fake_id)["reference"]

    # These zip code prefixes have too few people, so it might still be identifiable.
    # Instead, we'll zero out the zip for these areas.
    RESTRICTED_ZIPS = frozenset(
        [
            "036",
            "059",
            "102",
            "203",
            "205",
            "369",
            "556",
            "692",
            "821",
            "823",
            "878",
            "879",
            "884",
            "893",
        ]
    )
    ZIP_TRANSLATION = str.maketrans("123456789", "000000000")

    def _zip(self, value: str) -> str:
        first_three = value[:3]
        if first_three in self.RESTRICTED_ZIPS:
            first_three = "000"
        suffix = value[3:]
        # leave non-digit characters in place, so we don't trample over some non-US zips with chars
        return first_three + suffix.translate(self.ZIP_TRANSLATION)

    def _check_invalid_for_system(
        self, key: str, system: str | None, code: str | None, display: str | None
    ) -> bool:
        """
        Checks if any code/display fields that might be sensitive under some systems.

        Returns True if this should be stripped
        """
        code_ok, display_ok = self._is_system_allowed(system, code, display)
        return (key == "code" and not code_ok) or (key == "display" and not display_ok)

    @staticmethod
    def _is_system_allowed(
        system: str | None, code: str | None, display: str | None
    ) -> tuple[bool, bool]:
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
        if not system or not system.startswith("urn:oid:1.2.840.114350."):
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

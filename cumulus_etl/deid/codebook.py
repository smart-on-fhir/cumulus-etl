"""Codebook that stores the mappings between real and fake IDs"""

import binascii
import hmac
import logging
import os
import secrets
from collections.abc import Iterable, Iterator

from cumulus_etl import common


class Codebook:
    """
    Codebook links real IDs (like MRN medical record number) to fake IDs.
    Obtaining the fake ID without the codebook is safe.
    Codebook is saved local to the hospital and NOT shared on public internet.

    Some IDs may be cryptographically hashed versions of the real ID, some may be entirely random.

    Example usage as a context manager:

    with Codebook('/path/to/phi/') as codebook:
        print(codebook.fake_id("Patient", "1234"))

    At the end of the context manager, `save()` is called, which will cache mappings of any new
    patient/encounter IDs and preserve the hashing salt used to generate anonymized IDs (if given
    a fresh PHI folder).

    You can also use this class outside of a context manager, just be careful to call `save()`
    yourself.
    """

    def __init__(self, codebook_dir: str | None = None):
        """
        :param codebook_dir: saved codebook path or None (initialize empty)
        """
        self.db = CodebookDB(codebook_dir)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save()

    def save(self) -> bool:
        return self.db.save()

    def fake_id(self, resource_type: str | None, real_id: str, caching_allowed: bool = True) -> str:
        """
        Returns a new fake ID in place of the provided real ID

        This will always return the same fake ID for a given resource type & real ID, for the lifetime of the codebook.

        For some resource types (like Patient or Encounter), the result will be reversible.
        This is because we want to retain the ability to investigate oddities in the resulting Cumulus output.
        If we are seeing a signal in queries against the de-identified data, we want the hospital to be able to go back
        and investigate.
        Since it's reversible and preserved, that means we can also completely randomize it, since we have it sitting
        in memory anyway.

        But most other types don't need that reversibility and are instead cryptographically hashed using HMAC-SHA256
        and a random salt/secret. This is the same algorithm used by Microsoft's anonymization tools for FHIR.
        We use a hash rather than a stored mapping purely for memory reasons.

        :param resource_type: the FHIR resource name (e.g. Encounter)
        :param real_id: the actual Resource.id value for the original resource (i.e. the PHI version)
        :param caching_allowed: whether the codebook can consider caching this ID mapping (it only does so for certain
                                resources anyway)
        :returns: an anonymous ID to use in place of the real ID
        """
        if resource_type == "Patient":
            return self.db.patient(real_id, cache_mapping=caching_allowed)
        elif resource_type == "Encounter":
            return self.db.encounter(real_id, cache_mapping=caching_allowed)
        else:
            return self.db.resource_hash(real_id)

    def real_ids(self, resource_type: str, fake_ids: Iterable[str]) -> Iterator[str]:
        """
        Reverse-maps a list of fake IDs into real IDs.

        This is an expensive operation, so only a bulk API is provided.
        """
        mapping = self.db.get_reverse_mapping(resource_type)
        for fake_id in fake_ids:
            real_id = mapping.get(fake_id)
            if real_id:
                yield real_id
            else:
                logging.warning(
                    "Real ID not found for anonymous %s ID %s. Ignoring.", resource_type, fake_id
                )


###############################################################################
#
# Database for the codebook
#
###############################################################################


class CodebookDB:
    """Class to hold codebook data and read/write it to storage"""

    def __init__(self, codebook_dir: str | None = None):
        """
        Create a codebook database.

        Preserves scientific accuracy of patient counting and linkage while preserving patient privacy.

        Codebook replaces sensitive PHI identifiers with DEID linked identifiers.
        https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2244902

        :param codebook_dir: folder to load from (optional)
        """
        self.settings = {
            # If you change the saved format, bump this number and add your new format loader in _load_saved()
            "version": 1,
        }
        self.cached_mapping = {
            "Patient": {},
            "Encounter": {},
        }
        self.codebook_dir = codebook_dir

        # Tracks whether we need to write out our settings or mappings
        self.settings_modified = False
        self.mappings_modified = False

        if codebook_dir:
            try:
                self._load_saved_settings(
                    common.read_json(os.path.join(codebook_dir, "codebook.json"))
                )
            except (FileNotFoundError, PermissionError):
                pass
            try:
                self.cached_mapping = common.read_json(
                    os.path.join(codebook_dir, "codebook-cached-mappings.json")
                )
            except (FileNotFoundError, PermissionError):
                pass

        # Initialize salt if we don't have one yet
        if "id_salt" not in self.settings:
            # Create a salt, used when hashing resource IDs.
            # Some prior art is Microsoft's anonymizer tool which uses a UUID4 salt (with 122 bits of entropy).
            # Since this is an important salt, it seems reasonable to do a bit more.
            # Python's docs for the secrets module recommend 256 bits, as of 2015.
            # The sha256 algorithm is sitting on top of this salt, and a key size equal to the output size is also
            # recommended, so 256 bits seem good (which is 32 bytes).
            self.settings["id_salt"] = secrets.token_hex(32)
            self.settings_modified = True

    def patient(self, real_id: str, cache_mapping: bool = True) -> str:
        """
        Get a fake ID for a FHIR Patient ID

        :param real_id: patient resource ID
        :param cache_mapping: whether to cache the mapping
        :return: fake ID
        """
        return self._preserved_resource_hash("Patient", real_id, cache_mapping)

    def encounter(self, real_id: str, cache_mapping: bool = True) -> str:
        """
        Get a fake ID for a FHIR Encounter ID

        :param real_id: encounter resource ID
        :param cache_mapping: whether to cache the mapping
        :return: fake ID
        """
        return self._preserved_resource_hash("Encounter", real_id, cache_mapping)

    def _preserved_resource_hash(
        self, resource_type: str, real_id: str, cache_mapping: bool
    ) -> str:
        """
        Get a hashed ID and preserve the mapping.

        If an existing legacy random (non-hashed) ID is found, that is used instead. We used to do make this kind of
        random ID for Encounters and Patients, but using hashes means there is fewer disparate bits of code writing
        to the codebook, and we can more easily separate out the ID mappings to a separate file that can be deleted.

        :param resource_type: FHIR resource name
        :param real_id: patient resource ID
        :param cache_mapping: whether to cache the mapping
        :return: fake ID
        """
        # We used to store random (not hash-based) mappings in the codebook itself.
        # See if we have such a legacy mapping and use that if so, to not break existing data.
        # TODO: remove this path at some point. It's believed only BCH is using this.
        fake_id = self.settings.get(resource_type, {}).get(real_id)
        if fake_id:
            return fake_id

        # Fall back to a normal resource hash
        fake_id = self.resource_hash(real_id)

        # Save this generated ID mapping so that we can store it for debugging purposes later.
        # Only save if we don't have a legacy mapping, so that we don't have both in memory at the same time.
        if (
            cache_mapping
            and self.cached_mapping.setdefault(resource_type, {}).get(real_id) != fake_id
        ):
            # We expect the IDs to always be identical. The above check is mostly concerned with None != fake_id,
            # but is written defensively in case a bad mapping got saved for some reason.
            self.cached_mapping[resource_type][real_id] = fake_id
            self.mappings_modified = True

        return fake_id

    def get_reverse_mapping(self, resource_type: str) -> dict[str, str]:
        """
        Returns reversed cached mappings for a given resource.

        This is used for reverse-engineering anonymous IDs to the original real IDs, for the resources we cache.
        """
        mapping = self.cached_mapping.get(resource_type, {})
        reverse_mapping = {v: k for k, v in mapping.items()}

        # Add any legacy mappings from settings (iteratively, to avoid a spare version in memory)
        for k, v in self.settings.get(resource_type, {}).items():
            reverse_mapping[v] = k

        return reverse_mapping

    def resource_hash(self, real_id: str) -> str:
        """
        Get a fake ID for an arbitrary FHIR resource ID

        :param real_id: resource ID
        :return: hashed ID, using the saved salt
        """
        # This will be exactly 64 characters long, the maximum FHIR id length
        return hmac.new(self._id_salt(), digestmod="sha256", msg=real_id.encode("utf8")).hexdigest()

    def _id_salt(self) -> bytes:
        """Returns the saved salt or creates and saves one if needed"""
        salt = self.settings["id_salt"]
        # revert from doubled hex 64-char string representation back to just 32 bytes
        return binascii.unhexlify(salt)

    def _load_saved_settings(self, saved: dict) -> None:
        """
        :param saved: dictionary of preserved settings (like salt, version)
        """
        version = saved.get("version", 0)
        if version == 0:
            self._load_version0_settings(saved)
        elif version == 1:
            self._load_version1_settings(saved)
        else:
            raise ValueError(f'Unknown codebook version: "{version}"')

    def _load_version0_settings(self, saved: dict) -> None:
        """Loads version 0 of the codebook database format"""
        self.settings["Patient"] = {}
        self.settings["Encounter"] = {}
        for patient_id, patient_data in saved["mrn"].items():
            self.settings["Patient"][patient_id] = patient_data["deid"]

            for enc_id, enc_data in patient_data.get("encounter", {}).items():
                self.settings["Encounter"][enc_id] = enc_data["deid"]

    def _load_version1_settings(self, saved: dict) -> None:
        """Loads version 1 of the codebook database format"""
        self.settings = saved

    def save(self) -> bool:
        """
        Save the CodebookDB database as JSON
        :returns: whether a save actually happened (if codebook hasn't changed, nothing is written back)
        """
        if not self.codebook_dir:
            return False

        saved = False

        if self.settings_modified:
            codebook_path = os.path.join(self.codebook_dir, "codebook.json")
            common.write_json(codebook_path, self.settings)
            self.settings_modified = False
            saved = True

        if self.mappings_modified:
            cached_mapping_path = os.path.join(self.codebook_dir, "codebook-cached-mappings.json")
            common.write_json(cached_mapping_path, self.cached_mapping)
            self.mappings_modified = False
            saved = True

        return saved

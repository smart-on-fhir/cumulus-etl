"""Codebook that stores the mappings between real and fake IDs"""

import binascii
import hmac
import logging
import secrets

from cumulus import common


class Codebook:
    """
    Codebook links real IDs (like MRN medical record number) to fake IDs.
    Obtaining the fake ID without the codebook is safe.
    Codebook is saved local to the hospital and NOT shared on public internet.

    Some IDs may be cryptographically hashed versions of the real ID, some may be entirely random.
    """

    def __init__(self, saved: str = None):
        """
        :param saved: saved codebook path or None (initialize empty)
        """
        try:
            self.db = CodebookDB(saved)
        except (FileNotFoundError, PermissionError):
            self.db = CodebookDB()

    def fake_id(self, resource_type: str, real_id: str) -> str:
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
        """
        if resource_type == "Patient":
            return self.db.patient(real_id)
        elif resource_type == "Encounter":
            return self.db.encounter(real_id)
        else:
            return self.db.resource_hash(real_id)


###############################################################################
#
# Database for the codebook
#
###############################################################################


class CodebookDB:
    """Class to hold codebook data and read/write it to storage"""

    def __init__(self, saved: str = None):
        """
        Create a codebook database.

        Preserves scientific accuracy of patient counting and linkage while preserving patient privacy.

        Codebook replaces sensitive PHI identifiers with DEID linked identifiers.
        https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2244902

        :param saved: filename to load from (optional)
        """
        self.mapping = {
            # If you change the saved format, bump this number and add your new format loader in _load_saved()
            "version": 1,
            "Patient": {},
            "Encounter": {},
        }
        self.modified = True

        if saved:
            self._load_saved(common.read_json(saved))
            self.modified = False

    def patient(self, real_id: str) -> str:
        """
        Get a fake ID for a FHIR Patient ID

        :param real_id: patient resource ID
        :return: fake ID
        """
        return self._fake_id("Patient", real_id)

    def encounter(self, real_id: str) -> str:
        """
        Get a fake ID for a FHIR Encounter ID

        :param real_id: encounter resource ID
        :return: fake ID
        """
        return self._fake_id("Encounter", real_id)

    def _fake_id(self, resource_type: str, real_id: str) -> str:
        """
        Get a random fake ID and preserve the mapping
        :param resource_type: FHIR resource name
        :param real_id: patient resource ID
        :return: fake ID
        """
        type_mapping = self.mapping.setdefault(resource_type, {})

        fake_id = type_mapping.get(real_id)
        if not fake_id:
            # We could have just called setdefault() above instead of get(), but to avoid calling fake_id() more often
            # than necessary, only create new IDs as needed.
            # The ID does not need to be cryptographically random, since the real_id is not encoded in it at all.
            fake_id = common.fake_id(resource_type)
            type_mapping[real_id] = fake_id
            self.modified = True

        return fake_id

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
        salt = self.mapping.get("id_salt")

        if salt is None:
            # Create a salt, used when hashing resource IDs.
            # Some prior art is Microsoft's anonymizer tool which uses a UUID4 salt (with 122 bits of entropy).
            # Since this is an important salt, it seems reasonable to do a bit more.
            # Python's docs for the secrets module recommend 256 bits, as of 2015.
            # The sha256 algorithm is sitting on top of this salt, and a key size equal to the output size is also
            # recommended, so 256 bits seem good (which is 32 bytes).
            salt = secrets.token_hex(32)
            self.mapping["id_salt"] = salt
            self.modified = True

        return binascii.unhexlify(salt)  # revert from doubled hex 64-char string representation back to just 32 bytes

    def _load_saved(self, saved: dict) -> None:
        """
        :param saved: dictionary containing structure
                      [patient][encounter]
        :return:
        """
        version = saved.get("version", 0)
        if version == 0:
            self._load_version0(saved)
        elif version == 1:
            self._load_version1(saved)
        else:
            raise Exception(f'Unknown codebook version: "{version}"')

    def _load_version0(self, saved: dict) -> None:
        """Loads version 0 of the codebook database format"""
        for patient_id, patient_data in saved["mrn"].items():
            self.mapping["Patient"][patient_id] = patient_data["deid"]

            for enc_id, enc_data in patient_data.get("encounter", {}).items():
                self.mapping["Encounter"][enc_id] = enc_data["deid"]

    def _load_version1(self, saved: dict) -> None:
        """Loads version 1 of the codebook database format"""
        self.mapping = saved

    def save(self, path: str) -> bool:
        """
        Save the CodebookDB database as JSON
        :param path: /path/to/codebook.json
        :returns: whether a save actually happened (if codebook hasn't changed, nothing is written back)
        """
        if self.modified:
            logging.info("Saving codebook to: %s", path)
            common.write_json(path, self.mapping)
            self.modified = False
            return True
        else:
            return False

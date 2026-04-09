"""Abstraction for where to write and read data"""

import cumulus_fhir_support as cfs


def set_user_fs_options(args: dict) -> None:
    """Records user arguments that can affect filesystem options (like s3_region)"""
    cfs.FsPath.register_options(kms_key=args.get("s3_kms_key"), region=args.get("s3_region"))

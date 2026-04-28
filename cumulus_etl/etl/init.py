```python
"""Initialize an S3 path for Cumulus ETL data storage."""

import logging
import os
import sys
from typing import Optional

import boto3
import botocore.exceptions

from cumulus_etl import common, errors

logger = logging.getLogger(__name__)


async def init_command(
    target: str,
    *,
    kms_key: Optional[str] = None,
    profile: Optional[str] = None,
    region: Optional[str] = None,
) -> None:
    """
    Prepare an S3 directory for Cumulus ETL.

    Creates the bucket if it does not exist and validates
    that the provided KMS key (if any) is usable.

    Args:
        target: An S3 URI like s3://bucket/prefix/
        kms_key: Optional KMS key ARN or ID for server-side encryption
        profile: Optional AWS named profile
        region: Optional AWS region

    Raises:
        SystemExit: On any AWS authentication or permissions error
    """
    # Set up a boto3 session respecting the profile and region.
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    if region:
        session_kwargs["region_name"] = region

    # Build a session (credentials will be resolved lazily).
    try:
        session = boto3.Session(**session_kwargs)
        s3 = session.resource("s3")
        kms = session.client("kms")
    except Exception as exc:
        errors.fatal(
            f"Unable to create AWS session: {exc}\n"
            "Please check your AWS credentials, "
            "set the AWS_PROFILE environment variable, "
            "or use the --profile option."
        )

    # Parse bucket and prefix from the target S3 URI.
    bucket, prefix = common.parse_s3_path(target)
    if not bucket:
        errors.fatal(f"Not an S3 path: {target}")

    # --- Ensure the bucket exists (and validate basic credentials) ---
    try:
        # head_bucket is a lightweight credentialed call.
        s3.meta.client.head_bucket(Bucket=bucket)
        logger.info("Bucket %s already exists.", bucket)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code == "404":
            # Bucket doesn't exist; try to create it.
            try:
                s3.create_bucket(Bucket=bucket)
                logger.info("Created bucket %s.", bucket)
            except botocore.exceptions.ClientError as create_exc:
                errors.fatal(
                    f"Failed to create bucket '{bucket}': {create_exc}\n"
                    "Check that the bucket name is available and you have "
                    "the required permissions."
                )
        elif error_code == "403":
            errors.fatal(
                f"Access denied for bucket '{bucket}'.\n"
                "Ensure your AWS credentials are correct and you have "
                "s3:ListBucket permission on that bucket."
            )
        else:
            errors.fatal(
                f"Unexpected error when accessing bucket '{bucket}': {exc}"
            )
    except botocore.exceptions.NoCredentialsError:
        errors.fatal(
            "No AWS credentials found.\n"
            "Please configure your credentials by setting the "
            "AWS_PROFILE environment variable, using the --profile "
            "command line flag, or by running `aws configure`."
        )
    except botocore.exceptions.CredentialRetrievalError as exc:
        errors.fatal(
            f"Could not retrieve AWS credentials: {exc}\n"
            "Check your profile, environment variables, or IAM role."
        )
    except Exception as exc:
        errors.fatal(
            f"Unexpected error when validating S3 access: {exc}"
        )

    # --- If a KMS key was provided, validate it ---
    if kms_key:
        try:
            # describe_key will fail if the key doesn't exist or is inaccessible.
            kms.describe_key(KeyId=kms_key)
            logger.info("KMS key %s is valid and accessible.", kms_key)
        except botocore.exceptions.ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code in ("NotFoundException", "AccessDeniedException",
                              "InvalidKeyUsageException",
                              "KMSInvalidStateException",
                              "ValidationException"):
                errors.fatal(
                    f"Invalid or inaccessible KMS key '{kms_key}': {exc}\n"
                    "Please double-check the key ARN or ID passed via --s3-kms-key."
                )
            else:
                raise
        except botocore.exceptions.NoCredentialsError:
            errors.fatal(
                "No AWS credentials found while validating KMS key.\n"
                "Please configure your credentials by setting the "
                "AWS_PROFILE environment variable, using the --profile "
                "command line flag, or by running `aws configure`."
            )
        except botocore.exceptions.CredentialRetrievalError as exc:
            errors.fatal(
                f"Could not retrieve AWS credentials for KMS validation: {exc}\n"
                "Check your profile, environment variables, or IAM role."
            )

    # All good – write out an .init_complete marker (or similar) to confirm.
    try:
        marker_obj = s3.Object(bucket, f"{prefix}.cumulus-init")
        marker_obj.put(Body=b"initialized")
        logger.info("Init completed successfully for %s", target)
    except Exception as exc:
        errors.fatal(
            f"Failed to write initialization marker to {target}: {exc}\n"
            "Check your write permissions for this bucket and prefix."
        )
```
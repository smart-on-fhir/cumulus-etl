"""HTTP client that talk to a FHIR server"""

import argparse
from collections.abc import Iterable

import cumulus_fhir_support as cfs

from cumulus_etl import errors, fhir, store
from cumulus_etl.fhir import fhir_utils


def create_fhir_client_for_cli(
    args: argparse.Namespace,
    root_input: store.Root,
    resources: Iterable[str],
) -> cfs.FhirClient:
    """
    Create a FhirClient instance, based on user input from the CLI.

    The usual FHIR server authentication options should be represented in args.
    """
    client_base_url = getattr(args, "fhir_url", None)
    if root_input.is_http:
        if client_base_url and not root_input.path.startswith(client_base_url):
            errors.fatal(
                "You provided both an input FHIR server and a different --fhir-url. "
                "Try dropping --fhir-url.",
                errors.ARGS_CONFLICT,
            )
        elif not client_base_url:
            # Use the input URL as the base URL. But note that it may not be the server root.
            # For example, it may be a Group export URL. Let's try to find the actual root.
            client_base_url = fhir_utils.FhirUrl(root_input.path).root_url

    client_resources = set(resources)

    # If resources have other linked resources, add them to the scope,
    # since we'll usually need to download the referenced content.
    client_resources |= fhir.linked_resources(client_resources)

    try:
        return cfs.FhirClient.create_for_cli(
            client_base_url,
            client_resources,
            basic_user=args.basic_user,
            basic_password=args.basic_passwd,
            bearer_token=args.bearer_token,
            smart_client_id=args.smart_client_id,
            # Check deprecated --smart-jwks argument first
            smart_key=args.smart_jwks if args.smart_jwks else args.smart_key,
        )
    except cfs.AuthError as exc:
        errors.fatal(str(exc), errors.ARGS_INVALID)

"""Aid manual chart review by sending docs to Label Studio"""

import argparse

from cumulus import cli_utils, common, errors, fhir_client, loaders, store
from cumulus.chart_review import downloader


#####################################################################################################################
#
# DocumentReference gathering
#
#####################################################################################################################


async def gather_docrefs(
    client: fhir_client.FhirClient, root_input: store.Root, args: argparse.Namespace
) -> loaders.Directory:
    """Selects and downloads just the docrefs we need to a folder."""
    root_phi = store.Root(args.dir_phi, create=True)
    is_fhir_server = root_input.protocol == "https"

    if args.docrefs and args.anon_docrefs:
        errors.fatal("You cannot use both --docrefs and --anon-docrefs at the same time.", errors.ARGS_CONFLICT)

    ndjson_loader = loaders.FhirNdjsonLoader(root_input, client, export_to=args.export_to)

    # There are three possibilities: we have real IDs, we have fake IDs, or neither.
    if is_fhir_server:
        if args.docrefs:
            return await downloader.download_docrefs_from_real_ids(client, args.docrefs, export_to=args.export_to)
        elif args.anon_docrefs:
            return await downloader.download_docrefs_from_fake_ids(
                client, root_phi.path, args.anon_docrefs, export_to=args.export_to
            )
        else:
            # else we'll download the entire target path as a bulk export (presumably the user has scoped a Group)
            return await ndjson_loader.load_all(["DocumentReference"])
    else:
        # TODO: filter out to just the docrefs given by --docrefs etc
        return await ndjson_loader.load_all(["DocumentReference"])


#####################################################################################################################
#
# Main
#
#####################################################################################################################


def define_chart_review_parser(parser: argparse.ArgumentParser) -> None:
    parser.usage = "%(prog)s [OPTION]... INPUT PHI"

    parser.add_argument("dir_input", metavar="/path/to/input")
    parser.add_argument("dir_phi", metavar="/path/to/phi")

    parser.add_argument(
        "--export-to", metavar="PATH", help="Where to put exported documents (default is to delete after use)"
    )

    cli_utils.add_aws(parser)
    cli_utils.add_auth(parser)

    docs = parser.add_argument_group("document selection")
    docs.add_argument("--anon-docrefs", metavar="PATH", help="CSV file with anonymized patient_id,docref_id columns")
    docs.add_argument("--docrefs", metavar="PATH", help="CSV file with a docref_id column of original IDs")


async def chart_review_main(args: argparse.Namespace) -> None:
    """
    Prepare for chart review by uploading some documents to Label Studio.

    There are three major steps:
    1. Gather requested DocumentReference resources, reverse-engineering the original docref IDs if necessary
    2. Run NLP
    3. Run Philter
    4. Upload to Label Studio
    """
    common.set_user_fs_options(vars(args))  # record filesystem options like --s3-region before creating Roots

    root_input = store.Root(args.dir_input)
    client = fhir_client.create_fhir_client_for_cli(args, root_input, ["DocumentReference"])

    common.print_header("Chart review support is in development.\nPlease do not attempt to use for anything real.")

    async with client:
        common.print_header("Gathering documents:")
        await gather_docrefs(client, root_input, args)
        # TODO: run NLP
        # TODO: run philter
        # TODO: upload to Label Studio

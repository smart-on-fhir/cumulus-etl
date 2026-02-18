"""
Similar to a normal ETL task, but with an extra NLP focus.

Some differences:
- Runs only the NLP targeted tasks
- No completion tracking
- Leaves attachment data in place during scrubbing
- Has NLP specific arguments
"""

import argparse

import rich
import rich.prompt

from cumulus_etl import cli_utils, common, deid, nlp, store
from cumulus_etl.etl import pipeline
from cumulus_etl.etl.config import JobConfig


def define_nlp_parser(parser: argparse.ArgumentParser) -> None:
    """Fills out an argument parser with all the ETL options."""
    parser.usage = "%(prog)s [OPTION]... INPUT OUTPUT PHI"

    # Smaller default batch size than normal ETL because we might keep notes in memory during batch
    pipeline.add_common_etl_args(parser, batch_size=50_000)
    cli_utils.add_ctakes_override(parser)
    cli_utils.add_aws(parser, athena=True)
    nlp.add_note_selection(parser)

    parser.add_argument(
        "--task",
        action="append",
        help="run this NLP task (comma separated, use '--task help' to see full list)",
        required=True,
    )
    parser.add_argument(
        "--provider",
        choices=["azure", "bedrock", "local"],
        default="local",
        help="which model provider to use (default is local)",
    )
    parser.add_argument(
        "--azure-deployment",
        help="what Azure deployment name to use (default is model name)",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="use batching mode (saves money, might take longer, not all models support it)",
    )


async def check_input_size(
    codebook: deid.Codebook, folder: str, res_filter: deid.FilterFunc
) -> int:
    root = store.Root(folder)
    count = 0
    for resource in common.read_resource_ndjson(root, {"DiagnosticReport", "DocumentReference"}):
        if not res_filter or await res_filter(codebook, resource):
            count += 1
    return count


async def nlp_main(args: argparse.Namespace) -> None:
    job_datetime = common.datetime_now()
    scrubber, loader_results, selected_tasks = await pipeline.prepare_pipeline(
        args, job_datetime, nlp=True
    )

    # record CLI options for NLP models
    nlp.set_nlp_config(args)

    # Let the user know how many documents got selected, so there are no big cost surprises.
    res_filter = nlp.get_note_filter(args)
    count = await check_input_size(scrubber.codebook, loader_results.path, res_filter)
    response = cli_utils.prompt(f"Run NLP on {count:,} notes?", override=args.allow_large_selection)
    if response in cli_utils.PromptResponse.SKIPPED:
        rich.print(f"Running NLP on {count:,} notes…")

    # Prepare config for jobs
    config = JobConfig(
        args.dir_input,
        loader_results.path,
        args.dir_output,
        args.dir_phi,
        args.input_format,
        args.output_format,
        codebook_id=scrubber.codebook.get_codebook_id(),
        comment=args.comment,
        batch_size=args.batch_size,
        timestamp=job_datetime,
        dir_errors=args.errors_to,
        tasks=[t.name for t in selected_tasks],
        ctakes_overrides=args.ctakes_overrides,
        resource_filter=res_filter,
    )
    common.write_json(config.path_config(), config.as_json(), indent=4)

    # Finally, actually run the meat of the pipeline! (Filtered down to requested tasks)
    summaries = await pipeline.etl_job(config, selected_tasks, scrubber)

    pipeline.print_summary(summaries)


async def run_nlp(parser: argparse.ArgumentParser, argv: list[str]) -> None:
    """Parses an etl CLI"""
    define_nlp_parser(parser)
    args = parser.parse_args(argv)
    await nlp_main(args)

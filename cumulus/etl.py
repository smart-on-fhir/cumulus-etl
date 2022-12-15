"""Load, transform, and write out input data to deidentified FHIR"""

import argparse
import itertools
import json
import logging
import os
import re
import shutil
import socket
import sys
import tempfile
import time
from typing import Callable, Iterable, Iterator, List, Optional, Type, TypeVar
from urllib.parse import urlparse

import ctakesclient
import pandas
from fhirclient.models.condition import Condition
from fhirclient.models.documentreference import DocumentReference
from fhirclient.models.encounter import Encounter
from fhirclient.models.fhirabstractbase import FHIRAbstractBase
from fhirclient.models.observation import Observation
from fhirclient.models.patient import Patient
from fhirclient.models.resource import Resource

from cumulus import common, context, ctakes, deid, formats, loaders, store
from cumulus.config import JobConfig, JobSummary

###############################################################################
#
# Helpers
#
###############################################################################

T = TypeVar('T')
AnyFhir = TypeVar('AnyFhir', bound=FHIRAbstractBase)
AnyResource = TypeVar('AnyResource', bound=Resource)
DeidentifyCallable = Callable[[AnyFhir], bool]
StoreFormatCallable = Callable[[JobSummary, pandas.DataFrame, int], None]


def _read_ndjson(config: JobConfig, resource_type: Type[AnyResource]) -> Iterator[AnyResource]:
    """
    Grabs all ndjson files from a folder, of a particular resource type.

    Supports filenames like Condition.ndjson, Condition.000.ndjson, or 1.Condition.ndjson.
    """
    resource_name = resource_type.__name__

    pattern = re.compile(rf'([0-9]+.)?{resource_name}(.[0-9]+)?.ndjson')
    all_files = os.listdir(config.dir_input)
    filenames = filter(pattern.match, all_files)

    if not filenames:
        logging.error('Could not find any files for %s in the input folder, skipping that resource.', resource_name)
        return

    for filename in filenames:
        with common.open_file(os.path.join(config.dir_input, filename), 'r') as f:
            for line in f:
                yield resource_type(jsondict=json.loads(line), strict=False)


def _batch_iterate(iterable: Iterable[T], size: int) -> Iterator[Iterator[T]]:
    """
    Yields sub-iterators, each with {size} elements or less from iterable

    The whole iterable is never fully loaded into memory. Rather we load only one element at a time.

    Example:
        for batch in _batch_iterate([1, 2, 3, 4, 5], 2):
            print(list(batch))

    Results in:
        [1, 2]
        [3, 4]
        [5]
    """
    if size < 1:
        raise ValueError('Must iterate by at least a batch of 1')

    true_iterable = iter(iterable)  # in case it's actually a list (we want to iterate only once through)
    while True:
        iter_slice = itertools.islice(true_iterable, size)
        try:
            peek = next(iter_slice)
        except StopIteration:
            return  # we're done!
        yield itertools.chain([peek], iter_slice)


def _process_job_entries(
    config: JobConfig,
    job_name: str,
    resources: Iterator[Resource],
    to_deid: Optional[DeidentifyCallable],
    to_store: StoreFormatCallable,
):
    job = JobSummary(job_name)

    common.print_header(f'{job_name}()')

    # De-identify each entry by passing them through our scrubber
    if to_deid:
        deid_entries = filter(to_deid, resources)
    else:
        deid_entries = resources

    # At this point we have a giant iterable of de-identified FHIR objects, ready to be written out.
    # We want to batch them up, to allow resuming from interruptions more easily.
    for index, batch in enumerate(_batch_iterate(deid_entries, config.batch_size)):
        # Stuff de-identified FHIR json into one big pandas DataFrame
        dataframe = pandas.DataFrame(x.as_json() for x in batch)

        # Now we write that DataFrame to the target folder, in the requested format (e.g. parquet).
        to_store(job, dataframe, index)

    return job


###############################################################################
#
# FHIR Patient
#
###############################################################################


def etl_patient(config: JobConfig, scrubber: deid.Scrubber) -> JobSummary:
    return _process_job_entries(
        config,
        etl_patient.__name__,
        _read_ndjson(config, Patient),
        scrubber.scrub_resource,
        config.format.store_patients,
    )


###############################################################################
#
# FHIR Encounter
#
###############################################################################


def etl_encounter(config: JobConfig, scrubber: deid.Scrubber) -> JobSummary:
    return _process_job_entries(
        config,
        etl_encounter.__name__,
        _read_ndjson(config, Encounter),
        scrubber.scrub_resource,
        config.format.store_encounters,
    )


###############################################################################
#
# FHIR Observation (Lab Result)
#
###############################################################################


def etl_lab(config: JobConfig, scrubber: deid.Scrubber) -> JobSummary:
    return _process_job_entries(
        config,
        etl_lab.__name__,
        _read_ndjson(config, Observation),
        scrubber.scrub_resource,
        config.format.store_labs,
    )


###############################################################################
#
# FHIR Condition
#
###############################################################################


def etl_condition(config: JobConfig, scrubber: deid.Scrubber) -> JobSummary:
    return _process_job_entries(
        config,
        etl_condition.__name__,
        _read_ndjson(config, Condition),
        scrubber.scrub_resource,
        config.format.store_conditions,
    )


###############################################################################
#
# FHIR DocumentReference
#
###############################################################################

def etl_notes_meta(config: JobConfig, scrubber: deid.Scrubber) -> JobSummary:
    return _process_job_entries(
        config,
        etl_notes_meta.__name__,
        _read_ndjson(config, DocumentReference),
        scrubber.scrub_resource,
        config.format.store_docrefs,
    )


def load_nlp_symptoms(config: JobConfig, scrubber: deid.Scrubber) -> Iterator[Resource]:
    """Passes physician notes through NLP and returns any symptoms found"""
    for docref in _read_ndjson(config, DocumentReference):
        if not scrubber.scrub_resource(docref, scrub_attachments=False):
            continue
        symptoms = ctakes.symptoms(config.dir_phi, docref)
        for symptom in symptoms:
            yield symptom


def etl_notes_text2fhir_symptoms(config: JobConfig, scrubber: deid.Scrubber) -> JobSummary:
    return _process_job_entries(
        config,
        etl_notes_text2fhir_symptoms.__name__,
        load_nlp_symptoms(config, scrubber),
        None,  # scrubbing is done in load method
        config.format.store_symptoms,
    )


###############################################################################
#
# Main Pipeline (run all tasks)
#
###############################################################################

def load_and_deidentify(loader: loaders.Loader) -> tempfile.TemporaryDirectory:
    """
    Loads the input directory and does a first-pass de-identification

    Code outside this method should never see the original input files.

    :returns: a temporary directory holding the de-identified files in FHIR ndjson format
    """
    # First step is loading all the data into a local ndjson format
    loaded_dir = loader.load_all()

    # Second step is de-identifying that data (at a bulk level)
    return deid.Scrubber.scrub_bulk_data(loaded_dir.name)


def etl_job(config: JobConfig) -> List[JobSummary]:
    """
    :param config:
    :return:
    """
    summary_list = []

    task_list = [
        etl_patient,
        etl_encounter,
        etl_lab,
        etl_notes_meta,
        etl_notes_text2fhir_symptoms,
        etl_condition,
    ]

    scrubber = deid.Scrubber(config.path_codebook())
    for task in task_list:
        summary = task(config, scrubber)
        summary_list.append(summary)

        scrubber.save()

        path = os.path.join(config.dir_job_config(), f'{summary.label}.json')
        common.write_json(path, summary.as_json(), indent=4)

    return summary_list


###############################################################################
#
# External requirements (like cTAKES)
#
###############################################################################

def check_ctakes() -> None:
    """
    Verifies that cTAKES is available to receive requests.

    Will block while waiting for cTAKES.
    """
    # Check if our cTAKES server is ready (it may still not be fully ready once the socket is open, but at least it
    # will accept requests and then block the reply on it finishing its initialization)
    ctakes_url = ctakesclient.client.get_url_ctakes_rest()
    ctakes_url_parsed = urlparse(ctakes_url)

    num_tries = 6  # try six times / wait five seconds
    for i in range(num_tries):
        try:
            socket.socket().connect((ctakes_url_parsed.hostname, ctakes_url_parsed.port))
            break
        except ConnectionRefusedError:
            if i < num_tries - 1:
                time.sleep(1)
    else:
        print(f'A running cTAKES server was not found at:\n    {ctakes_url}\n\n'
              'Please set the URL_CTAKES_REST environment variable to your server.',
              file=sys.stderr)
        raise SystemExit(1)


def check_mstool() -> None:
    """
    Verifies that the MS anonymizer tool is installed in PATH.
    """
    if not shutil.which(deid.MSTOOL_CMD):
        print(f'No executable found for {deid.MSTOOL_CMD}.\n\n'
              'Please see https://github.com/microsoft/Tools-for-Health-Data-Anonymization\n'
              'and install it into your PATH.',
              file=sys.stderr)
        raise SystemExit(1)


def check_requirements() -> None:
    """
    Verifies that all external services and programs are ready

    May block while waiting a bit for them.
    """
    check_ctakes()
    check_mstool()


###############################################################################
#
# Main
#
###############################################################################

def main(args: List[str]):
    parser = argparse.ArgumentParser()
    parser.add_argument('dir_input', metavar='/path/to/input')
    parser.add_argument('dir_output', metavar='/path/to/processed')
    parser.add_argument('dir_phi', metavar='/path/to/phi')
    parser.add_argument('--input-format', default='ndjson', choices=['i2b2', 'ndjson'],
                        help='input format (default is ndjson)')
    parser.add_argument('--output-format', default='parquet', choices=['json', 'ndjson', 'parquet'],
                        help='output format (default is parquet)')
    parser.add_argument('--batch-size', type=int, metavar='SIZE', default=10000000,
                        help='how many entries to process at once and thus '
                             'how many to put in one output file (default is 10M)')
    parser.add_argument('--comment', help='add the comment to the log file')
    parser.add_argument('--s3-region', help='if using S3 paths (s3://...), this is their region')
    parser.add_argument('--s3-kms-key', help='if using S3 paths (s3://...), this is the KMS key ID to use')
    parser.add_argument('--smart-client-id', metavar='CLIENT_ID', help='Client ID registered with SMART FHIR server '
                                                                       '(can be a filename with ID inside it')
    parser.add_argument('--smart-jwks', metavar='/path/to/jwks', help='JWKS file registered with SMART FHIR server')
    parser.add_argument('--skip-init-checks', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args(args)

    logging.info('Input Directory: %s', args.dir_input)
    logging.info('Output Directory: %s', args.dir_output)
    logging.info('PHI Build Directory: %s', args.dir_phi)

    # Check that cTAKES is running and any other services or binaries we require
    if not args.skip_init_checks:
        check_requirements()

    common.set_user_fs_options(vars(args))  # record filesystem options like --s3-region before creating Roots

    root_input = store.Root(args.dir_input)
    root_output = store.Root(args.dir_output, create=True)
    root_phi = store.Root(args.dir_phi, create=True)

    job_context = context.JobContext(root_phi.joinpath('context.json'))
    job_datetime = common.datetime_now()  # grab timestamp before we do anything

    if args.input_format == 'i2b2':
        config_loader = loaders.I2b2Loader(root_input)
    else:
        config_loader = loaders.FhirNdjsonLoader(root_input, client_id=args.smart_client_id, jwks=args.smart_jwks)

    if args.output_format == 'json':
        config_store = formats.JsonTreeFormat(root_output)
    elif args.output_format == 'parquet':
        config_store = formats.ParquetFormat(root_output)
    else:
        config_store = formats.NdjsonFormat(root_output)

    deid_dir = load_and_deidentify(config_loader)

    # Prepare config for jobs
    config = JobConfig(config_loader, deid_dir.name, config_store, root_phi, comment=args.comment,
                       batch_size=args.batch_size, timestamp=job_datetime)
    common.write_json(config.path_config(), config.as_json(), indent=4)
    common.print_header('Configuration:')
    print(json.dumps(config.as_json(), indent=4))

    # Finally, actually run the meat of the pipeline!
    summaries = etl_job(config)

    # Print results to the console
    common.print_header('Results:')
    for summary in summaries:
        print(json.dumps(summary.as_json(), indent=4))

    # Update job context for future runs
    job_context.last_successful_datetime = job_datetime
    job_context.last_successful_input_dir = root_input.path
    job_context.last_successful_output_dir = root_output.path
    job_context.save()


def main_cli():
    main(sys.argv[1:])


if __name__ == '__main__':
    main_cli()

"""Load, transform, and write out input data to deidentified FHIR"""

import argparse
import itertools
import json
import logging
import os
import socket
import sys
import time
from functools import partial
from typing import Callable, Iterable, Iterator, List, Optional, TypeVar
from urllib.parse import urlparse

import ctakesclient
import pandas
from fhirclient.models.fhirabstractbase import FHIRAbstractBase

from cumulus import common, ctakes, deid, formats, loaders, store
from cumulus.config import JobConfig, JobSummary
from cumulus.loaders import ResourceIterator

###############################################################################
#
# Helpers
#
###############################################################################

T = TypeVar('T')
AnyResource = TypeVar('AnyResource', bound=FHIRAbstractBase)
LoaderCallable = Callable[[], ResourceIterator]
DeidentifyCallable = Callable[[AnyResource], bool]
StoreFormatCallable = Callable[[JobSummary, pandas.DataFrame, int], None]


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
    loader: LoaderCallable,
    to_deid: Optional[DeidentifyCallable],
    to_store: StoreFormatCallable,
):
    job = JobSummary(job_name)

    print('###############################################################')
    print(f'{job_name}()')

    # Load input data into an iterable of FHIR objects
    fhir_entries = loader()

    # De-identify each entry by passing them through our scrubber
    if to_deid:
        deid_entries = filter(to_deid, fhir_entries)
    else:
        deid_entries = fhir_entries

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
        config.loader.load_patients,
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
        config.loader.load_encounters,
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
        config.loader.load_labs,
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
        config.loader.load_conditions,
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
        config.loader.load_docrefs,
        scrubber.scrub_resource,
        config.format.store_docrefs,
    )


def load_nlp_symptoms(config: JobConfig, scrubber: deid.Scrubber) -> ResourceIterator:
    """Passes physician notes through NLP and returns any symptoms found"""
    for docref in config.loader.load_docrefs():
        if not scrubber.scrub_resource(docref, scrub_attachments=False):
            continue
        symptoms = ctakes.symptoms(config.dir_phi, docref)
        for symptom in symptoms:
            yield symptom


def etl_notes_text2fhir_symptoms(config: JobConfig, scrubber: deid.Scrubber) -> JobSummary:
    return _process_job_entries(
        config,
        etl_notes_text2fhir_symptoms.__name__,
        partial(load_nlp_symptoms, config, scrubber),
        None,  # scrubbing is done in load method
        config.format.store_symptoms,
    )


###############################################################################
#
# Main Pipeline (run all tasks)
#
###############################################################################


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
        print(f'A running cTAKES server was not found at:\n    {ctakes_url}\n'
              'Please set the URL_CTAKES_REST environment variable to your server.',
              file=sys.stderr)
        raise SystemExit(1)


def check_requirements() -> None:
    """
    Verifies that all external services and programs are ready

    May block while waiting a bit for them.
    """
    check_ctakes()


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
    parser.add_argument('--output-format', default='ndjson', choices=['json', 'ndjson', 'parquet'],
                        help='output format (default is ndjson)')
    parser.add_argument('--batch-size', type=int, metavar='SIZE', default=10000000,
                        help='how many entries to process at once and thus '
                             'how many to put in one output file (default is 10M)')
    parser.add_argument('--comment', help='add the comment to the log file')
    parser.add_argument('--skip-init-checks', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args(args)

    logging.info('Input Directory: %s', args.dir_input)
    logging.info('Output Directory: %s', args.dir_output)
    logging.info('PHI Build Directory: %s', args.dir_phi)

    # Check that cTAKES is running and any other services or binaries we require
    if not args.skip_init_checks:
        check_requirements()

    root_input = store.Root(args.dir_input, create=True)
    root_output = store.Root(args.dir_output, create=True)
    root_phi = store.Root(args.dir_phi, create=True)

    if args.input_format == 'i2b2':
        config_loader = loaders.I2b2Loader(root_input)
    else:
        config_loader = loaders.FhirNdjsonLoader(root_input)

    if args.output_format == 'json':
        config_store = formats.JsonTreeFormat(root_output)
    elif args.output_format == 'parquet':
        config_store = formats.ParquetFormat(root_output)
    else:
        config_store = formats.NdjsonFormat(root_output)

    config = JobConfig(config_loader, config_store, root_phi, comment=args.comment, batch_size=args.batch_size)
    print(json.dumps(config.as_json(), indent=4))

    common.write_json(config.path_config(), config.as_json(), indent=4)

    for summary in etl_job(config):
        print(json.dumps(summary.as_json(), indent=4))


def main_cli():
    main(sys.argv[1:])


if __name__ == '__main__':
    main_cli()

"""Load, transform, and write out input data to deidentified FHIR"""

import argparse
import itertools
import json
import logging
import os
import sys
from functools import partial
from typing import Callable, Iterable, Iterator, List, TypeVar, Union

import pandas
from fhirclient.models.resource import Resource

from cumulus import common, formats, store
from cumulus import i2b2
from cumulus.codebook import Codebook
from cumulus.config import JobConfig, JobSummary
from cumulus.i2b2.schema import Dimension as I2b2Dimension

###############################################################################
#
# Helpers
#
###############################################################################

T = TypeVar('T')
AnyResource = TypeVar('AnyResource', bound=Resource)
AnyDimension = TypeVar('AnyDimension', bound=I2b2Dimension)
CsvToI2b2Callable = Callable[[str], Iterable[I2b2Dimension]]
I2b2ToFhirCallable = Callable[[AnyDimension], Union[Resource, List[Resource]]]
DeidentifyCallable = Callable[[AnyResource], Resource]
StoreFormatCallable = Callable[[JobSummary, pandas.DataFrame, int], None]


def _extract_from_files(extract: CsvToI2b2Callable, csv_files: Iterable[str]) -> Iterator[I2b2Dimension]:
    """Generator method that lazily loads input csv files"""
    for csv_file in csv_files:
        for entry in extract(csv_file):
            yield entry


def _flatten(iterable: Iterable) -> Iterator:
    """
    Generator that flattens any lists it finds into individual objects

    That is, _flatten([1, [2, 3], 4]) will yield [1, 2, 3, 4].
    Note:
        - this only flattens explicit `list` types, not any iterable it finds
        - this only goes one level deep
    """
    for item in iterable:
        if isinstance(item, list):
            for sub_item in item:
                yield sub_item
        else:
            yield item


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
    csv_folder: str,
    extract: CsvToI2b2Callable,
    to_fhir: I2b2ToFhirCallable,
    deid: DeidentifyCallable,
    to_store: StoreFormatCallable,
):
    job = JobSummary(job_name)

    print('###############################################################')
    print(f'{job_name}()')

    # Convert input folder full of i2b2 csv files into an iterable of FHIR objects
    i2b2_csv_files = config.list_csv(csv_folder)
    i2b2_entries = _extract_from_files(extract, i2b2_csv_files)
    fhir_entries = _flatten(to_fhir(x) for x in i2b2_entries)

    # De-identify each entry by passing them through our codebook
    deid_entries = (deid(x) for x in fhir_entries)

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


def etl_patient(config: JobConfig, codebook: Codebook) -> JobSummary:
    return _process_job_entries(
        config,
        etl_patient.__name__,
        'csv_patient',
        i2b2.extract.extract_csv_patients,
        i2b2.transform.to_fhir_patient,
        codebook.fhir_patient,
        config.format.store_patients,
    )


###############################################################################
#
# FHIR Encounter
#
###############################################################################


def etl_visit(config: JobConfig, codebook: Codebook) -> JobSummary:
    return _process_job_entries(
        config,
        etl_visit.__name__,
        'csv_visit',
        i2b2.extract.extract_csv_visits,
        i2b2.transform.to_fhir_encounter,
        codebook.fhir_encounter,
        config.format.store_encounters,
    )


###############################################################################
#
# FHIR Observation (Lab Result)
#
###############################################################################


def etl_lab(config: JobConfig, codebook: Codebook) -> JobSummary:
    return _process_job_entries(
        config,
        etl_lab.__name__,
        'csv_lab',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.to_fhir_observation_lab,
        codebook.fhir_observation,
        config.format.store_labs,
    )


###############################################################################
#
# FHIR Condition
#
###############################################################################


def etl_diagnosis(config: JobConfig, codebook: Codebook) -> JobSummary:
    return _process_job_entries(
        config,
        etl_diagnosis.__name__,
        'csv_diagnosis',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.to_fhir_condition,
        codebook.fhir_condition,
        config.format.store_conditions,
    )


###############################################################################
#
# FHIR DocumentReference
#
###############################################################################

def etl_notes_meta(config: JobConfig, codebook: Codebook) -> JobSummary:
    return _process_job_entries(
        config,
        etl_notes_meta.__name__,
        'csv_note',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.to_fhir_documentreference,
        codebook.fhir_documentreference,
        config.format.store_docrefs,
    )


def etl_notes_text2fhir_symptoms(config: JobConfig, codebook: Codebook) -> JobSummary:
    return _process_job_entries(
        config,
        etl_notes_text2fhir_symptoms.__name__,
        'csv_note',
        i2b2.extract.extract_csv_observation_facts,
        partial(i2b2.transform.text2fhir_symptoms, config.dir_phi),
        codebook.fhir_observation,
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
        etl_visit,
        etl_lab,
        etl_notes_meta,
        etl_notes_text2fhir_symptoms,
        etl_diagnosis,
    ]

    codebook = Codebook(config.path_codebook())
    for task in task_list:
        summary = task(config, codebook)
        summary_list.append(summary)

        codebook.db.save(config.path_codebook())

        path = os.path.join(config.dir_job_config(), f'{summary.label}.json')
        common.write_json(path, summary.as_json(), indent=4)

    return summary_list


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
    parser.add_argument('--format', default='json', choices=['json', 'ndjson', 'parquet'],
                        help='output format (default is json)')
    parser.add_argument('--batch-size', type=int, metavar='SIZE', default=10000000,
                        help='how many entries to process at once and thus '
                             'how many to put in one output file (default is 10M)')
    parser.add_argument('--comment', help='add the comment to the log file')
    args = parser.parse_args(args)

    logging.info('Input Directory: %s', args.dir_input)
    logging.info('Output Directory: %s', args.dir_output)
    logging.info('PHI Build Directory: %s', args.dir_phi)

    root_input = store.Root(args.dir_input, create=True)
    root_output = store.Root(args.dir_output, create=True)
    root_phi = store.Root(args.dir_phi, create=True)

    if args.format == 'ndjson':
        config_store = formats.NdjsonFormat(root_output)
    elif args.format == 'parquet':
        config_store = formats.ParquetFormat(root_output)
    else:
        config_store = formats.JsonTreeFormat(root_output)

    config = JobConfig(root_input, root_phi, config_store, comment=args.comment, batch_size=args.batch_size)
    print(json.dumps(config.as_json(), indent=4))

    common.write_json(config.path_config(), config.as_json(), indent=4)

    for summary in etl_job(config):
        print(json.dumps(summary.as_json(), indent=4))


def main_cli():
    main(sys.argv[1:])


if __name__ == '__main__':
    main_cli()

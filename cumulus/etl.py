"""Load, transform, and write out input data to deidentified FHIR"""

import argparse
import json
import logging
import os
import sys
from typing import Callable, Iterable, Iterator, List, TypeVar, Union

import pandas
from fhirclient.models.resource import Resource

from cumulus import common, store, store_json_tree, store_ndjson, store_parquet
from cumulus import i2b2
from cumulus.codebook import Codebook
from cumulus.config import JobConfig, JobSummary
from cumulus.i2b2.schema import Dimension as I2b2Dimension

###############################################################################
#
# Helpers
#
###############################################################################

AnyResource = TypeVar('AnyResource', bound=Resource)
AnyDimension = TypeVar('AnyDimension', bound=I2b2Dimension)
CsvToI2b2Callable = Callable[[str], Iterable[I2b2Dimension]]
I2b2ToFhirCallable = Callable[[AnyDimension], Union[Resource, List[Resource]]]
DeidentifyCallable = Callable[[Codebook, Union[AnyResource, List[AnyResource]]], Resource]
StoreFormatCallable = Callable[[JobSummary, pandas.DataFrame], None]


def _extract_from_files(extract: CsvToI2b2Callable, csv_files: Iterable[str]) -> Iterator[I2b2Dimension]:
    """Generator method that lazily loads input csv files"""
    for csv_file in csv_files:
        for entry in extract(csv_file):
            yield entry


def _deid_to_json(obj):
    """Returns a json-style structure from an object or list of objects"""
    if isinstance(obj, Resource):
        return obj.as_json()

    # Else iterate and recurse
    return (_deid_to_json(x) for x in obj)


def _process_job_entries(
    config: JobConfig,
    job_name: str,
    csv_folder: str,
    extract: CsvToI2b2Callable,
    to_fhir: I2b2ToFhirCallable,
    deid: DeidentifyCallable,
    to_store: StoreFormatCallable,
):
    codebook = Codebook(config.path_codebook())

    job = JobSummary(job_name)

    print('###############################################################')
    print(f'{job_name}()')

    i2b2_csv_files = config.list_csv(csv_folder)
    i2b2_entries = _extract_from_files(extract, i2b2_csv_files)
    fhir_entries = (to_fhir(x) for x in i2b2_entries)
    deid_entries = (deid(codebook, x) for x in fhir_entries)
    dataframe = pandas.DataFrame(_deid_to_json(x) for x in deid_entries)

    to_store(job, dataframe)

    codebook.db.save(config.path_codebook())
    return job


###############################################################################
#
# FHIR Patient
#
###############################################################################


def etl_patient(config: JobConfig) -> JobSummary:
    return _process_job_entries(
        config,
        etl_patient.__name__,
        'csv_patient',
        i2b2.extract.extract_csv_patients,
        i2b2.transform.to_fhir_patient,
        Codebook.fhir_patient,
        config.format.store_patients,
    )


###############################################################################
#
# FHIR Encounter
#
###############################################################################


def etl_visit(config: JobConfig) -> JobSummary:
    return _process_job_entries(
        config,
        etl_visit.__name__,
        'csv_visit',
        i2b2.extract.extract_csv_visits,
        i2b2.transform.to_fhir_encounter,
        Codebook.fhir_encounter,
        config.format.store_encounters,
    )


###############################################################################
#
# FHIR Observation (Lab Result)
#
###############################################################################


def etl_lab(config: JobConfig) -> JobSummary:
    return _process_job_entries(
        config,
        etl_lab.__name__,
        'csv_lab',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.to_fhir_observation_lab,
        Codebook.fhir_observation,
        config.format.store_labs,
    )


###############################################################################
#
# FHIR Condition
#
###############################################################################


def etl_diagnosis(config: JobConfig) -> JobSummary:
    return _process_job_entries(
        config,
        etl_diagnosis.__name__,
        'csv_diagnosis',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.to_fhir_condition,
        Codebook.fhir_condition,
        config.format.store_conditions,
    )


###############################################################################
#
# FHIR DocumentReference
#
###############################################################################

def etl_notes_meta(config: JobConfig) -> JobSummary:
    return _process_job_entries(
        config,
        etl_notes_meta.__name__,
        'csv_note',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.to_fhir_documentreference,
        Codebook.fhir_documentreference,
        config.format.store_docrefs,
    )


def etl_notes_text2fhir_symptoms(config: JobConfig) -> JobSummary:
    return _process_job_entries(
        config,
        etl_notes_text2fhir_symptoms.__name__,
        'csv_note',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.text2fhir_symptoms,
        Codebook.fhir_observation_list,
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

    for task in task_list:
        summary = task(config)
        summary_list.append(summary)

        path = os.path.join(config.dir_job_config(), f'{summary.label}.json')
        common.write_json(path, summary.as_json())

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
    parser.add_argument('--format',
                        choices=['json', 'ndjson', 'parquet'],
                        default='json')
    parser.add_argument('--comment', help='add the comment to the log file')
    args = parser.parse_args(args)

    logging.info('Input Directory: %s', args.dir_input)
    logging.info('Output Directory: %s', args.dir_output)
    logging.info('PHI Build Directory: %s', args.dir_phi)

    root_input = store.Root(args.dir_input, create=True)
    root_output = store.Root(args.dir_output, create=True)
    root_phi = store.Root(args.dir_phi, create=True)

    if args.format == 'ndjson':
        config_store = store_ndjson.NdjsonFormat(root_output)
    elif args.format == 'parquet':
        config_store = store_parquet.ParquetFormat(root_output)
    else:
        config_store = store_json_tree.JsonTreeFormat(root_output)

    config = JobConfig(root_input, root_phi, config_store, comment=args.comment)
    print(json.dumps(config.as_json(), indent=4))

    common.write_json(config.path_config(), config.as_json())

    for summary in etl_job(config):
        print(json.dumps(summary.as_json(), indent=4))


def main_cli():
    main(sys.argv[1:])


if __name__ == '__main__':
    main_cli()

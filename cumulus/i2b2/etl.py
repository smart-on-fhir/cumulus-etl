"""Load, transform, and write out i2b2 to FHIR"""

import argparse
import json
import logging
import os
import sys
from typing import Any, Callable, List

import pandas
from fhirclient.models.documentreference import DocumentReference

from cumulus import common, store, store_json_tree, store_ndjson, store_parquet
from cumulus import i2b2
from cumulus.i2b2.config import JobConfig, JobSummary
from cumulus.codebook import Codebook

###############################################################################
#
# Helpers
#
###############################################################################


def _extract_from_files(extract: Callable[[str], List[Any]],
                        csv_files: List[str]):
    """Generator method that lazily loads input csv files"""
    for csv_file in csv_files:
        for entry in extract(csv_file):
            yield entry


def _process_job_entries(
    config: JobConfig,
    job_name: str,
    csv_folder: str,
    extract: Callable[[str], List[Any]],
    to_fhir: Callable[[Any], Any],
    deid: Callable[[Codebook, Any], Any],
    to_store: Callable[[JobSummary, pandas.DataFrame], None],
):
    codebook = Codebook(config.path_codebook())

    job = JobSummary(job_name)

    print('###############################################################')
    print(f'{job_name}()')

    i2b2_csv_files = config.list_csv(csv_folder)
    i2b2_entries = _extract_from_files(extract, i2b2_csv_files)
    fhir_entries = (to_fhir(x) for x in i2b2_entries)
    deid_entries = (deid(codebook, x) for x in fhir_entries)
    dataframe = pandas.DataFrame(x.as_json() for x in deid_entries)

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
        'etl_patient',
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
        'etl_visit',
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
        'etl_lab',
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
        'etl_diagnosis',
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


def _strip_notes_from_docref(codebook: Codebook,
                             docref: DocumentReference) -> DocumentReference:
    codebook.fhir_documentreference(docref)

    for content in docref.content:
        content.attachment.data = None

    return docref


def etl_notes_meta(config: JobConfig) -> JobSummary:
    return _process_job_entries(
        config,
        'etl_notes_meta',
        'csv_note',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.to_fhir_documentreference,
        # Make sure no notes get through as docrefs (they come via store_notes)
        _strip_notes_from_docref,
        config.format.store_docrefs,
    )


def etl_notes_text2fhir_symptoms(config: JobConfig) -> JobSummary:
    return _process_job_entries(
        config,
        'etl_notes_text2fhir_symptoms',
        'csv_note',
        i2b2.extract.extract_csv_observation_facts,
        i2b2.transform.text2fhir_symptoms,
        Codebook.fhir_observation_list,
        config.format.store_observation_list,
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
        i2b2.etl.etl_patient,
        i2b2.etl.etl_visit,
        i2b2.etl.etl_lab,
        i2b2.etl.etl_notes_meta,
        # i2b2.etl.etl_notes_text2fhir_symptoms, TODO: tests will fail currently without mock server.
        i2b2.etl.etl_diagnosis,
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
    parser.add_argument('dir_input', metavar='/my/i2b2/input')
    parser.add_argument('dir_output', metavar='/my/i2b2/processed')
    parser.add_argument('dir_phi', metavar='/my/i2b2/phi')
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


if __name__ == '__main__':
    main(sys.argv[1:])

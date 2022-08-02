import os, sys
import json
from typing import List
import logging
import pandas
import i2b2
import codebook
import ctakes_client

def path_for_result(out_dir:str, observation: i2b2.ObservationFact):
    """
    :param out_dir: directory to save cTAKES result
    :param observation:
    :return: path to cTAKES results
    """
    # practical limit of number of "files" in a folder is 10,000
    root = str(observation.patient_num)
    if len(str(observation.patient_num)) >= 4:
        root = root[0:4]

    pat = os.path.join(out_dir, root, observation.patient_num)
    enc = os.path.join(pat, observation.encounter_num)
    concept = os.path.join(enc, observation.concept_cd)

    if not os.path.exists(pat): os.makedirs(pat)
    if not os.path.exists(enc): os.makedirs(enc)
    if not os.path.exists(concept): os.makedirs(concept)

    return concept

def etl(notes_csv:str, out_dir:str) -> List[i2b2.ObservationFact]:
    """
    Extract Transform Load
    Extract: from CSV file of Observation Fact
    Transform: real PHI identifiers to DEID fake identifiers
    Load: save JSON result
    :param notes_csv: path to CSV file containing physician notes
    :param out_dir: path to directory results from cTAKES
    :return: list of ObservationFacts that were processed by ETL (without note text).
    """
    logging.info(f'Reading csv {notes_csv} ...')
    df = pandas.read_csv(notes_csv, dtype=str)
    logging.info(f'Done reading {notes_csv} .')

    processed = list()

    count = df.shape[0]

    for index, row in df.iterrows():

        logging.debug(f'index {index} row {row} ... ')

        observation = i2b2.ObservationFact(row)

        current = path_for_result(out_dir, observation)

        if os.path.exists(os.path.join(current, 'codebook.json')):
            logging.info(f'codebook.json already exists for {current}')
        else:
            try:
                res = ctakes_client.call_ctakes(observation.observation_blob)

                with open(os.path.join(current, 'ctakes.json'), 'w') as f:
                    f.write(json.dumps(res, indent=4))

                with open(os.path.join(current, 'codebook.json'), 'w') as f:
                   f.write(json.dumps(
                       codebook.deid_i2b2(observation).__dict__, indent=4))

                with open(os.path.join(current, 'i2b2.json'), 'w') as f:
                    observation.observation_blob = None
                    f.write(json.dumps(
                        observation.__dict__, indent=4))

            except Exception as e:
                logging.error(f'During call_ctakes {e}')
                logging.error(f'{observation}')

                with open(os.path.join(current, 'error.txt'), 'w') as f:
                    f.write(f'{e}')

        if index % 100 == 0:
            progress = float(index / count)
            logging.info(f'total= {count}, processed={index}, progress % = {progress}')

    return processed


def debug_mode():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

def info_mode():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

def main(args):
    if len(args) < 2:
        print('usage')
        print('example: /my/i2b2/observation_fact/notes.csv /my/i2b2/observation_fact/output/')
    else:
        notes_csv = args[0]
        output_dir = args[1]
        info_mode()
        #debug_mode()
        logging.info(f"Physician Notes CSV file: {notes_csv}")
        logging.info(f"Output Directory: {output_dir}")
        etl(notes_csv, output_dir)


if __name__ == '__main__':
    main(sys.argv[1:])


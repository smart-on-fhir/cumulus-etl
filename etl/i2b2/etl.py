import sys
from typing import List
import logging
import i2b2
from i2b2 import extract
import pipeline

class Job:
    def __init__(self, inpath:str, outpath=str, command=None, sample=1.0):
        """
        :param inpath: input path (CSV file or other resource)
        :param outpath: output path (folder or other resource)
        :param command: method to run
        :param sample: % percentage of the dataset to process
        """
        self.inpath = inpath
        self.outpath = outpath
        self.command = command
        self.sample = sample

def etl_docref(notes_path:str, out_dir:str, command= pipeline.PipeCTAKES(), sample=1.0) -> List:
    """
    Inspired by "command" design pattern 
    https://en.wikipedia.org/wiki/Command_pattern 
    
    Extract Transform Load
    Extract: List[Observation Fact]
    Transform: real PHI identifiers to DEID fake identifiers
    Load: save JSON result
    
    :param notes_path: path to CSV file containing physician notes
    :param out_dir: path to directory results from cTAKES
    :param command: Task operation to run over the collection
    :param sample: % of rows to sample (default 100%)
    :return: list of ObservationFacts that were processed by ETL (without note text).
    """
    df = extract.extract_csv(notes_path, sample)

    processed = list()

    for index, row in df.iterrows():
        observation = i2b2.ObservationFact(row)

        try:
            res = command.pipe(out_dir, observation)
            logging.info(res)

            processed.append(res)

        except Exception as e:
            logging.error(e)
            pipeline.PipeLogError().pipe(out_dir, observation)

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
        etl_docref(notes_csv, output_dir)


if __name__ == '__main__':
    main(sys.argv[1:])


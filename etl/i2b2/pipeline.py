import logging
from etl.i2b2 import load
from etl import deid, codebook
import bsv
import ctakes
from i2b2.schema import ObservationFact


class Pipe:
    """
    'Queue system' layer inspired by RMQ which is cloud agnostic.
    SQS (simple queue service) by AWS could be easily swapped in, as could
    other Queue systems that follow the publish/consume topic pattern.
    """
    def pipe(self, root, obs: ObservationFact):
        logging.fatal('no default implementation')

#######################################################################################################################
#
# Codebook / DEID Commands
#
#######################################################################################################################

class PipeCodebook(Pipe):

    def pipe(self, root, obs: ObservationFact):
        """
        :param root: path root, currently filesystem
        :param obs: patient data including note
        :return: path to codebook.json
        """
        path = load.path_codebook(root)

        if not load.path_exists(path):
            logging.info('creating empty codebook.json')
            logging.info(load.write(path, codebook.Codebook().__dict__))

        cb = codebook.Codebook(load.read(path))

        cb.encounter(mrn=obs.patient_num,
                     encounter_id= obs.encounter_num,
                     period_start=obs.start_date,
                     period_end=obs.end_date)

        cb.docref(mrn=obs.patient_num,
                  encounter_id= obs.encounter_num,
                  md5sum= deid.hash_clinical_text(obs.observation_blob))

        return load.write(path, cb.__dict__)

class PipePhilter(Pipe):

    def pipe(self, root, obs: ObservationFact):
        logging.fatal('no implementation.')
        redacted = deid.philter(obs.observation_blob)
        #
        # ...
        #


#######################################################################################################################
#
# Error Logging
#
#######################################################################################################################

class PipeLogError(Pipe):

    def pipe(self, root, obs: ObservationFact):
        """
        :param root: path root, currently filesystem
        :param obs: patient data including note
        :return: error message
        """
        path = load.path_error(root)
        if not load.path_exists(path):
            load.write(path, {'error': list()})

        saved = load.read(path)

        if not obs:
            message = 'observation was null'
        elif not isinstance(obs, dict):
            message = f'observation was not a dictionary, got {obs}'
        else:
            message = obs.__dict__

        saved['error'].append(message)

        return message

#######################################################################################################################
#
# Clinical Text requests/responses
#
#######################################################################################################################

class PipeCTAKES(Pipe):

    def pipe(self, root, obs: ObservationFact):
        """
        :param root: path root, currently filesystem
        :param obs: patient data including note
        :return: path to ctakes.json
        """
        path = load.path_ctakes(root, obs)

        if len(obs.observation_blob) < 4:
            logging.warning(f'Text was too short: {obs.observation_blob}')
            return None

        if load.path_exists(path):
            logging.info(f'exists, skipping: {path}')
            return path
        else:
            return load.write(path=path, message=ctakes.call_ctakes(obs.observation_blob))

class PipeBSV(Pipe):

    def __init__(self, semtype=ctakes.UmlsTypeMention.SignSymptom):
        """
        :param semtype: Semantic Type to read from cTAKES JSON Result
        """
        self.semtype = semtype

    def pipe(self, root, obs: ObservationFact):
        ctakes_json = load.path_ctakes(root, obs)

        if load.path_exists(ctakes_json):
            res = load.read(ctakes_json)
            symptoms = bsv.res_to_bsv(res, self.semtype)

            if len(symptoms) > 0:
                path = load.path_bsv_semtype(root, obs, self.semtype)
                return bsv.bsv_to_file(symptoms, path)

class PipeConcatBSV(PipeBSV):

    def __init__(self, semtype=ctakes.UmlsTypeMention.SignSymptom):
        self.semtype = semtype

    def pipe(self, root, obs: ObservationFact):
        import os
        path = load.path_bsv_semtype(root, obs, self.semtype)
        path_concat = os.path.join(root, f'{self.semtype}.bsv')

        if load.path_exists(path):
            with open(path, 'r') as src:
                content = src.read()

                with open(path_concat, 'a') as concat:
                    concat.write('\n')
                    concat.write(content)

                concat.close()
            src.close()
            return path_concat

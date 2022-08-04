import logging
import store
import deid
import codebook
import i2b2
import ctakes

class Pipe:
    """
    'Queue system' layer inspired by RMQ which is cloud agnostic.
    SQS (simple queue service) by AWS could be easily swapped in, as could
    other Queue systems that follow the publish/consume topic pattern.
    """
    def pipe(self, root, obs: i2b2.ObservationFact):
        logging.fatal('no default implementation')


class PipeCTAKES(Pipe):

    def pipe(self, root, obs: i2b2.ObservationFact):
        """
        :param root: path root, currently filesystem
        :param obs: patient data including note
        :return: path to ctakes.json
        """
        path = store.path_ctakes(root, obs)

        if store.path_exists(path):
            logging.info(f'exists, skipping: {path}')
            return path
        else:
            return store.write(path=path, message=ctakes.call_ctakes(obs.observation_blob))


class PipeCodebook(Pipe):

    def pipe(self, root, obs: i2b2.ObservationFact):
        """
        :param root: path root, currently filesystem
        :param obs: patient data including note
        :return: path to codebook.json
        """
        path = store.path_codebook(root)

        if not store.path_exists(path):
            logging.info('creating empty codebook.json')
            logging.info(store.write(path, codebook.Codebook().__dict__))

        cb = codebook.Codebook(store.read(path))

        cb.encounter(mrn=obs.patient_num,
                     encounter_id= obs.encounter_num,
                     period_start=obs.start_date,
                     period_end=obs.end_date)

        cb.docref(mrn=obs.patient_num,
                  encounter_id= obs.encounter_num,
                  md5sum= deid.hash_clinical_text(obs.observation_blob))

        return store.write(path, cb.__dict__)


class PipeLogError(Pipe):

    def pipe(self, root, obs: i2b2.ObservationFact):
        """
        :param root: path root, currently filesystem
        :param obs: patient data including note
        :return: error message
        """
        path = store.path_error(root)
        if not store.path_exists(path):
            store.write(path, {'error': list()})

        saved = store.read(path)

        if not obs:
            message = 'observation was null'
        elif not isinstance(obs, dict):
            message = f'observation was not a dictionary, got {obs}'
        else:
            message = obs.__dict__

        saved['error'].append(message)

        return message

class PipePhilter(Pipe):

    def pipe(self, root, obs: i2b2.ObservationFact):
        logging.fatal('no implementation.')
        redacted = deid.philter(obs.observation_blob)
        #
        # ...
        #













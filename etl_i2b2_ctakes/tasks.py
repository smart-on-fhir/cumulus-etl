import logging
import store
import ctakes_client
import codebook
import i2b2

class Task:
    """
    'Queue system' layer inspired by RMQ which is cloud agnostic.
    SQS (simple queue service) by AWS could be easily swapped in, as could
    other Queue systems that follow the publish/consume topic pattern.
    """
    def publish(self, root, obs: i2b2.ObservationFact):
        logging.fatal('no default implementation')


class TaskCTAKES(Task):

    def publish(self, root, obs: i2b2.ObservationFact):
        """
        :param root: path root, currently filesystem
        :param obs: patient data including note
        :return: path to ctakes.json
        """
        path = store.path_note(root, obs)

        if store.path_exists(path):
            logging.info(f'exists, skipping: {path}')
            return path
        else:
            return store.write(path=path, message=ctakes_client.call_ctakes(obs.observation_blob))


class TaskCodebook(Task):

    def publish(self, root, obs: i2b2.ObservationFact):
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
                  md5sum= codebook.hash_clinical_text(obs.observation_blob))

        return store.write(path, cb.__dict__)







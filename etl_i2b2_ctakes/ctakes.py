import os
import logging
import requests
from typing import List
from enum import Enum

#######################################################################################################################
#
# Semantic Types
#
#######################################################################################################################
class SemType(Enum):
    DiseaseDisorder = 'DiseaseDisorderMention'
    SignSymptom = 'SignSymptomMention'
    AnatomicSite = 'AnatomicalSiteMention'
    Medication = 'MedicationMention'
    Procedure = 'ProcedureMention'
    Identified = 'IdentifiedAnnotation'

#######################################################################################################################
#
# HTTP Client for CTAKES REST
#
#######################################################################################################################

def get_url_ctakes() -> str:
    """
    :return: CTAKES_URL_REST env variable or default using localhost
    """
    return os.environ.get('CTAKES_URL_REST', 'http://localhost:8080/ctakes-web-rest/service/analyze')

def call_ctakes(sentence:str, url=get_url_ctakes()) -> dict:
    """
    :param sentence: clinical text to send to cTAKES
    :param url: cTAKES REST server fully qualified path
    :return:
    """
    logging.debug(url)
    return requests.post(url, data=sentence).json()


#######################################################################################################################
#
# BSV Bar|Separated|Values
#
#######################################################################################################################

class BSV:
    """
    BSV = "Bar|Separated|Values"
    BSV = "CODE|CUI|STR"
    https://ctakes.apache.org/apidocs/3.2.2/org/apache/ctakes/dictionary/lookup2/dictionary/BsvRareWordDictionary.html
    """
    def __init__(self, code=None, cui=None, text=None):
        """
        :param code: CODE or "identified annotation" code
        :param cui: CUI from UMLS or NA for "identified annotation"
        :param text: string representation to send to ctakes
        """
        self.code = code
        self.cui = cui
        self.text = text

def list_bsv(path:str) -> List[BSV]:
    """
    :param path: BSV filename to parse
    :return: list of BSV entries
    """
    entries = list()

    with open(path) as f:
        for line in f.read().splitlines():
            cols = line.split('|')
            entries.append(BSV(code= cols[0], cui=cols[1], text=cols[2]))

    return entries

def res_to_bsv(response:dict, sem_type:SemType)-> List[BSV]:
    """
    :param response: cTAKES response
    :param sem_type: Semantic Type (Group)
    :return: List of BSV entries from cTAKES
    """
    bsv_res = list()

    for atts in response.get(sem_type.value, []):
        for concept in atts['conceptAttributes']:
            bsv_res.append(BSV(concept['code'], concept['cui'], atts['text']))
    return bsv_res
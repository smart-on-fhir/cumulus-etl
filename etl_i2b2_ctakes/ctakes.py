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
    AnatomicalSite = 'AnatomicalSiteMention'
    Medication = 'MedicationMention'
    Procedure = 'ProcedureMention'
    CustomDict = 'IdentifiedAnnotation'

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


class UMLSLookup():
    pass

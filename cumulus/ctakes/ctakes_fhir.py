from enum import Enum
from fhirclient.models.extension import Extension

#######################################################################################################################
#
# NLP Extension
#
#######################################################################################################################

class NLPExtension(Enum):
    nlp_text_position = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position'
    nlp_algorithm = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-algorithm'
    nlp_polarity = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity'


def ext_text_position(pos_begin:int, pos_end:int) -> Extension:
    """
    :param pos_begin: character position
    :param pos_end: character position
    :return:
    """
    ext_begin = Extension({'valueInteger':pos_begin, 'url': 'begin'})
    ext_end = Extension({'valueInteger': pos_end, 'url': 'end'})
    return Extension({'url': NLPExtension.nlp_text_position.value,
                      'extension': [ext_begin.as_json(), ext_end.as_json()]})

def ext_algorithm(url=NLPExtension.nlp_algorithm.value, dateofauthorship='2021-06-23') -> Extension:
    """
    :param url: defines the NLP algorithm.
    :param dateofauthorship: defines when the NLP algorithm date is effective.
    :return:
    """
    dateofauthorship = Extension({'url': 'dateofauthorship', 'valueDate': dateofauthorship})

    return Extension({'url': url, 'extension': [dateofauthorship]})

def ext_polarity(positive=True) -> Extension:
    return Extension

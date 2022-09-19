"""NLP extension using ctakes"""

from enum import Enum
from fhirclient.models.extension import Extension


class NLPExtension(Enum):
    # pylint: disable=line-too-long
    NLP_TEXT_POSITION = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position'
    NLP_ALGORITH = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-algorithm'
    NLP_POLARITY = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity'


def ext_text_position(pos_begin: int, pos_end: int) -> Extension:
    """
    :param pos_begin: character position
    :param pos_end: character position
    :return:
    """
    ext_begin = Extension({'valueInteger': pos_begin, 'url': 'begin'})
    ext_end = Extension({'valueInteger': pos_end, 'url': 'end'})
    return Extension({
        'url': NLPExtension.NLP_TEXT_POSITION.value,
        'extension': [ext_begin.as_json(),
                      ext_end.as_json()]
    })


def ext_algorithm(url=NLPExtension.NLP_ALGORITH.value,
                  dateofauthorship='2021-06-23') -> Extension:
    """
    :param url: defines the NLP algorithm.
    :param dateofauthorship: defines when the NLP algorithm date is effective.
    :return:
    """
    dateofauthorship = Extension({
        'url': 'dateofauthorship',
        'valueDate': dateofauthorship
    })

    return Extension({'url': url, 'extension': [dateofauthorship]})


def ext_polarity(positive=True) -> Extension:
    del positive
    return Extension

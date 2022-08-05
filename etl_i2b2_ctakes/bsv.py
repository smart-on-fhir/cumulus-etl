from typing import List

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
        self.code = code if code else ''
        self.cui = cui if cui else ''
        self.text = text if text else ''

    def __str__(self):
        """
        :return: CODE|CUI|STR
        """
        safetext = self.text.replace('\n', '').replace('\r', '')
        return f"{self.code}|{self.cui}|{safetext}"

def res_to_bsv(response:dict, sem_type:str) -> List[BSV]:
    """
    :param response: cTAKES response
    :param sem_type: Semantic Type (Group)
    :return: List of BSV entries from cTAKES
    """
    bsv_res = list()

    for atts in response.get(sem_type, []):
        for concept in atts['conceptAttributes']:
            bsv_res.append(BSV(concept['code'], concept['cui'], atts['text']))
    return bsv_res

def bsv_to_str(bsv) -> str:
    """
    :param bsv: BSV or List[BSV]
    :return: CODE|CUI|STR \n CODE|CUI|STR \n ...
    """
    if isinstance(bsv, list):
        rows = [str(r) for r in bsv]
        return '\n'.join(rows)
    else:
        return str(bsv)+"\n"

def file_to_bsv(path:str) -> List[BSV]:
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

def str_to_bsv_list(content:str) -> List[BSV]:
    items = list()
    for line in content.splitlines():
        cols = line.split('|')
        items.append(BSV(code= cols[0], cui=cols[1], text=cols[2]))
    return items

def bsv_to_file(bsv, path:str) -> str:
    """
    :param bsv: BSV or List[BSV]
    :param path: save BSV to this path
    :return: path to BSV file
    """
    with open(path, 'a') as f:
        f.write(bsv_to_str(bsv))
    return path
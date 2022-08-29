import os, sys
from os.path import join, exists, isfile
import requests
import json

# @author Tim Miller, PhD
# Boston Children's Hospital

ss_ache = "Muscle or body aches"
ss_anosmia = 'New loss of taste or smell'
ss_congestion = 'Congestion or runny nose'
ss_cough = 'Cough'
ss_diarrhea = 'Diarrhea'
ss_fatigue = 'Fatigue'
ss_fever_chills = 'Fever/chills'
ss_headache = 'Headache'
ss_nausea = 'Nausea or vomiting'
ss_sob = 'Shortness of breath/difficulty breathing'
ss_throat = 'Sore throat'

ss_cui_maps = {
    'C0231528': ss_ache,
    'C0281856': ss_ache,
    'C0003126': ss_anosmia,
    'C2364111': ss_anosmia,
    'C0700148': ss_congestion,
    'C1260880': ss_congestion,
    'C0010200': ss_cough,
    'C0011991': ss_diarrhea,
    'C0015672': ss_fatigue,
    'C0015967': ss_fever_chills,
    'C0424755': ss_fever_chills,
    'C0085593': ss_fever_chills,
    'C0085594': ss_fever_chills,
    'C0018681': ss_headache,
    'C0027497': ss_nausea,
    'C0027498': ss_nausea,
    'C0013404': ss_sob,
    'C0231807': ss_sob,
    'C0242429': ss_throat,
    'C0031350': ss_throat
}


def main(args):
    if len(args) < 1:
        sys.stderr.write('One required argument(s): <input directory>\n')
        sys.exit(-1)

    url = 'http://localhost:8080/ctakes-web-rest/service/analyze'

    preannot = []

    for fn in os.listdir(args[0]):
        fpath = join(args[0], fn)
        if isfile(fpath):

            doc_annots = {'data': {}, 'predictions': [{'model_version': 'ctakes-covid', 'result': []}]}
            with open(fpath, 'rt') as f:
                text = f.read()
            doc_annots['data']['text'] = text
            doc_labels = set()

            r = requests.post(url, data=text, params={'format': 'filtered'})
            if r.status_code == 200:
                ctakes_annotations = r.json()['_views']['_InitialView']
                ss = ctakes_annotations['SignSymptomMention']
                ss_id = 0
                for symptom in ss:
                    # Add NER spans:
                    begin_offset = symptom['begin']
                    end_offset = symptom['end']
                    negated = symptom['polarity'] == -1
                    ss_text = text[begin_offset:end_offset]
                    ss_annot = {'id': 'ss%d' % ss_id,
                                'from_name': 'label',
                                'to_name': 'text',
                                'type': 'labels',
                                'value': {
                                    'start': begin_offset,
                                    'end': end_offset,
                                    'score': 1.0,
                                    'text': ss_text,
                                    'labels': ['SignSymptomMention']
                                }
                                }
                    ss_id += 1
                    doc_annots['predictions'][0]['result'].append(ss_annot)

                    # Check whether this concept matches a document-level CUIs we are interested in:
                    for concept in symptom['ontologyConceptArr']:
                        cui = concept['cui']
                        if cui in ss_cui_maps and not negated:
                            doc_labels.add(ss_cui_maps[cui])

                # iterate over the document-level CUIs we found
                doc_ss_annot = {
                    'id': 'ss%d' % ss_id,
                    'from_name': 'symptoms',
                    'to_name': 'text',
                    'type': 'choices',
                    'value': {'choices': [ss_name for ss_name in doc_labels]}
                }
                doc_annots['predictions'][0]['result'].append(doc_ss_annot)

            preannot.append(doc_annots)
            # break

    print(json.dumps(preannot))


if __name__ == '__main__':
    main(sys.argv[1:])
import unittest

CC = ['Chief Complaint']

VISIT = ['Basic Information', 'MRN', 'FIN', 'DOB', 'Author']
HPI = ['History of Present Illness']
ROS = ['Review of Systems', 'ROS',
       'Constitutional symptoms',
       'Skin symptoms',
       'Respiratory symptoms',
       'Cardiovascular symptoms',
       'GI', 'Gastrointestinal symptoms',
       'Genitourinary symptoms',
       'Musculoskeletal symptoms',
       'Neurologic symptoms',
       'Endocrine symptoms']

STATUS = ['Health Status']

DRUG = ['Medications', 'Meds'
        'Prescriptions', 'Immunizations' 
        'Medications Prescribed', 'Medications Prescribed This Visit']

DRUG_ATTR = ['bid', 'b.i.d.', 'b.i.d', 'twice daily',
             'prn', 'p.r.n', 'as needed']

ALLERGY = ['Allergies', 'Allergic Reactions']
ALLERGY_NKDA = ['NKDA', 'No Known Drug Allergy', 'No Known Drug Allergies' 
                'No Known Medication Allergies']

PMH = ['PMH', 'Past Medical', 'Past Medical History', 'Medical History',
       'Problem List', 'Problem List/Past Medical History',
       'Ongoing', 'Historical',
       'Procedure History', 'Procedure/Surgical History']
FHX = ['FHX', 'Family History']
SHX = ['SHX', 'Social History']

PE = ['Physical Exam', 'Physical Examination',
      'Skin', 'Head', 'Neck', 'Eye', 'Eyes',
      'HEENT', 'ENT', 'Ears', 'Nose', 'Throat',
      'CV', 'Cards', 'Cardiovascular',
      'ABD', 'Abdomen',
      'Respiratory', 'Chest',
      'GI', 'Gastrointestinal',
      'MSK', 'Musculoskeletal'
      'Neuro', 'Neurological',
      'Lymph', 'Lymphatics',
      'Psych', 'Psychiatric']

VS = ['Vital Signs', 'VITALS',
      'Respiratory Rate',
      'Heart Rate', 'Blood Pressure',
      'SBP', 'Systolic', 'Systolic Blood Pressure',
      'DBP', 'Diastolic Blood Pressure']

DX_RESULT = ['Lab results', 'Lab View', 'Diagnostic Results']

RAD = ['Radiology', 'Radiology results']
PROC = ['Procedure']
DX_FINAL = ['Final Diagnosis']

RE_EVAL = ['Reexamination/Reevaluation', 'Reexamination', 'Reevaluation',
           'Attending Attestation', 'Attending', 'Attestation']

MDM = ['Medical Decision Making', 'Addendum']

A_P = ['Assessment and Plan', 'Assessment/Plan']
I_P = ['Impression and Plan', 'Impression/Plan', 'Impression']

DISCHARGE = ['Discharge Instructions', 'Follow up']

class TestSectionizer(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()

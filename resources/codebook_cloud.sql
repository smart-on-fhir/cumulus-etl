-- PHI Codebook hides Patient identifiers in a separate database
-- A Self-scaling, Distributed Information Architecture for Public Health, Research, and Clinical Care
-- http://www.ncbi.nlm.nih.gov/pmc/articles/PMC2244902

-- http://www.hl7.org/fhir/patient-operation-match.html

--FHIR Patient.id (mrn, ssn)
--FHIR Patient.name
--FHIR Patient.dob
--FHIR Patient.period
--FHIR Patient.gender (not PHI, useful for matching)
--FHIR Patient.contact
--FHIR Patient.postalCode
--FHIR Patient.address

drop   table if exists codebook_patient;

CREATE EXTERNAL TABLE
codebook_patient (
patient_uuid  string,   -- Random ID generation (https://docs.python.org/3/library/uuid.html)
mrn   string,   -- http://hl7.org/fhir/patient-definitions.html#Patient.identifier
ssn   string,   -- http://hl7.org/fhir/sid/us-ssn
dob date,
name_text   string, -- http://hl7.org/fhir/datatypes.html#HumanName
name_given  string,
name_family string,
name_prefix string,
name_suffix string,
period_start date,
period_end   date,
gender      string, -- not PHI, but useful for patient matching
contact_system string, -- http://hl7.org/fhir/ValueSet/contact-point-system
contact_value  string,
contact_use    string, -- http://hl7.org/fhir/ValueSet/contact-point-use
contact_rank   int, -- http://hl7.org/fhir/datatypes-definitions.html#ContactPoint.rank
address_text   string, -- http://www.hl7.org/fhir/patient-definitions.html#Patient.address
postalCode     string, -- rarely PHI (small #counts)
address_type   string, -- http://hl7.org/fhir/ValueSet/address-type
address_country string, -- Country (e.g. can be ISO 3166 2 or 3 letter code)
address_line   string,
address_state  string,
address_city   string -- rarely PHI (small #counts)
)
LOCATION 's3://cumulus/codebook/patient'

drop   table if exists codebook_encounter;

CREATE EXTERNAL TABLE
codebook_encounter (
    patient_uuid   string,  -- FK codebook_patient
    encounter_uuid string,  -- PK codebook_encounters
    encounter_num string, -- PHI encounter number
    enct_date_start date, -- LDS encounter start date with time
    enct_date_end date -- LDS encounter end date with time
)
LOCATION 's3://cumulus/codebook/encounter'

drop   table if exists codebook_docref;

CREATE EXTERNAL TABLE
codebook_docref (
    patient_uuid    string,  -- FK codebook_patient
    encounter_uuid  string,  -- PK codebook_encounters
    docref_uuid     string,   -- Random ID generation (https://docs.python.org/3/library/uuid.html)
    encounter_id    string,
    enct_date date
)
LOCATION 's3://cumulus/codebook/docref'
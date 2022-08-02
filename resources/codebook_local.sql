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
CREATE TABLE
codebook_patient (
patient_uuid  varchar(36),   -- Random ID generation (https://docs.python.org/3/library/uuid.html)
mrn   varchar(36),   -- http://hl7.org/fhir/patient-definitions.html#Patient.identifier
ssn   varchar(12),   -- http://hl7.org/fhir/sid/us-ssn
dob date,
name_text   varchar(255), -- http://hl7.org/fhir/datatypes.html#HumanName
name_given  varchar(255),
name_family varchar(255),
name_prefix varchar(255),
name_suffix varchar(255),
period_start date,
period_end   date,
gender      varchar(100), -- not PHI, but useful for patient matching
contact_system varchar(100), -- http://hl7.org/fhir/ValueSet/contact-point-system
contact_value  varchar(100),
contact_use    varchar(100), -- http://hl7.org/fhir/ValueSet/contact-point-use
contact_rank   int, -- http://hl7.org/fhir/datatypes-definitions.html#ContactPoint.rank
address_text   varchar(1000), -- http://www.hl7.org/fhir/patient-definitions.html#Patient.address
postalCode     varchar(5), -- rarely PHI (small #counts)
address_type   varchar(20), -- http://hl7.org/fhir/ValueSet/address-type
address_country varchar(100), -- Country (e.g. can be ISO 3166 2 or 3 letter code)
address_line   varchar(100),
address_state  varchar(100),
address_city   varchar(100) -- rarely PHI (small #counts)
);

drop   table if exists codebook_encounter;
CREATE TABLE
codebook_encounter (
    patient_uuid   varchar(36),  -- FK codebook_patient
    encounter_uuid varchar(36),  -- PK codebook_encounters
    encounter_num varchar(100), -- PHI encounter number
    enct_date_start date, -- LDS encounter start date with time
    enct_date_end date -- LDS encounter end date with time
);

drop   table if exists codebook_docref;
CREATE TABLE
codebook_docref (
    patient_uuid    varchar(36),  -- FK codebook_patient
    encounter_uuid  varchar(36),  -- PK codebook_encounters
    docref_uuid     varchar(36),   -- Random ID generation (https://docs.python.org/3/library/uuid.html)
    encounter_id    varchar(100),
    enct_date date
);
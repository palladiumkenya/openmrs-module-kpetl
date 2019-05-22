DROP PROCEDURE IF EXISTS create_etl_tables$$
CREATE PROCEDURE create_etl_tables()
BEGIN
DECLARE script_id INT(11);

-- create/recreate database kp_etl
drop database if exists kp_etl;
create database kp_etl;

drop database if exists kp_datatools;
create database kp_datatools;

DROP TABLE IF EXISTS kp_etl.etl_script_status;
CREATE TABLE kp_etl.etl_script_status(
  id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  script_name VARCHAR(50) DEFAULT null,
  start_time DATETIME DEFAULT NULL,
  stop_time DATETIME DEFAULT NULL,
  error VARCHAR(255) DEFAULT NULL
);

-- Log start time
INSERT INTO kp_etl.etl_script_status(script_name, start_time) VALUES('initial_creation_of_tables', NOW());
SET script_id = LAST_INSERT_ID();

DROP TABLE IF EXISTS kp_etl.etl_client_registration;
DROP TABLE IF EXISTS kp_etl.etl_client_social_status;
DROP TABLE IF EXISTS kp_etl.etl_client_enrollment;
/*DROP TABLE if exists kp_etl.etl_hiv_enrollment;
DROP TABLE IF EXISTS kp_etl.etl_patient_hiv_followup;
DROP TABLE IF EXISTS kp_etl.etl_laboratory_extract;
DROP TABLE IF EXISTS kp_etl.etl_pharmacy_extract;
DROP TABLE IF EXISTS kp_etl.etl_patient_treatment_event;
DROP TABLE IF EXISTS kp_etl.etl_patient_program_discontinuation;
DROP TABLE IF EXISTS kp_etl.etl_mch_enrollment;
DROP TABLE IF EXISTS kp_etl.etl_mch_antenatal_visit;
DROP TABLE IF EXISTS kp_etl.etl_mch_postnatal_visit;
DROP TABLE IF EXISTS kp_etl.etl_tb_enrollment;
DROP TABLE IF EXISTS kp_etl.etl_tb_follow_up_visit;
DROP TABLE IF EXISTS kp_etl.etl_tb_screening;
DROP TABLE IF EXISTS kp_etl.etl_hei_enrollment;
DROP TABLE IF EXISTS kp_etl.etl_hei_follow_up_visit;
DROP TABLE IF EXISTS kp_etl.etl_mchs_delivery;
DROP TABLE IF EXISTS kp_etl.etl_mchs_discharge;
DROP TABLE IF EXISTS kp_etl.etl_hei_immunization;
DROP TABLE IF EXISTS kp_etl.etl_patients_booked_today;
DROP TABLE IF EXISTS kp_etl.etl_missed_appointments;
DROP TABLE if exists kp_etl.etl_patient_demographics;
DROP TABLE IF EXISTS kp_etl.etl_drug_event;
DROP TABLE IF EXISTS kp_etl.etl_hts_test;
DROP TABLE IF EXISTS kp_etl.etl_hts_referral_and_linkage;
DROP TABLE IF EXISTS kp_etl.tmp_regimen_events_ordered;
DROP TABLE IF EXISTS kp_etl.etl_ccc_defaulter_tracing;
DROP TABLE IF EXISTS kp_etl.etl_ART_preparation;
DROP TABLE IF EXISTS kp_etl.etl_enhanced_adherence;
DROP TABLE IF EXISTS kp_etl.etl_patient_triage;
*/

-- create table etl_client_registration
create table kp_etl.etl_client_registration (
client_id INT(11) not null primary key,
unique_patient_no VARCHAR(50),
registration_date DATE,
given_name VARCHAR(255),
middle_name VARCHAR(255),
family_name VARCHAR(255),
unidentified int(11),
Gender VARCHAR(10),
DOB DATE,
alias_name VARCHAR(255),
postal_address VARCHAR (255),
county VARCHAR (255),
sub_county VARCHAR (255),
location VARCHAR (255),
sub_location VARCHAR (255),
village VARCHAR (255),
phone_number VARCHAR (255)  DEFAULT NULL,
alt_phone_number VARCHAR (255)  DEFAULT NULL,
email_address VARCHAR (255)  DEFAULT NULL,
national_id_number VARCHAR(50),
passport_number VARCHAR(50)  DEFAULT NULL,
dead INT(11),
death_date DATE DEFAULT NULL,
voided INT(11),
constraint foreign key(client_id) references provider(provider_id),
index(client_id),
index(unique_patient_no),
index(Gender),
index(DOB)

);

SELECT "Successfully created etl_client_registration table";

-- create table etl_client_social_status
create table kp_etl.etl_client_social_status (
uuid char(38) ,
client_id INT(11) NOT NULL,
visit_id INT(11) DEFAULT NULL,
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
encounter_provider INT(11),
key_population_type VARCHAR(255),
peer_educator int(11),
hot_spot VARCHAR(255),
weekly_sex_acts int (11),
weekly_anal_sex_acts int (11),
daily_drug_injections int (11),
avg_weekly_drug_injections int (11),
voided INT(11),
constraint foreign key(peer_educator) references kp_etl.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
index(client_id),
index(key_population_type)
);

SELECT "Successfully created etl_client_social_status table";

-- create table etl_client_enrollment

create table kp_etl.etl_client_enrollment (
uuid char(38) ,
unique_client_no VARCHAR(50) NOT NULL PRIMARY KEY,
client_id INT(11) NOT NULL,
visit_id INT(11) DEFAULT NULL,
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
encounter_provider INT(11),
enrollment_date DATETIME,
enrollment_service_area VARCHAR(255),
contacted_for_prevention int(11),
contact_person_name VARCHAR(255),
regular_free_sexual_partner int(11),
free_sexual_partner_name VARCHAR(255),
free_sexual_partner_alias VARCHAR(255),
free_sexual_partner_contact VARCHAR(255),
contact_regular_sexual_partner int(11),
sex_work_startdate DATE,
sex_with_men_startdate DATE,
drug_startdate DATE,
sexual_violence_experienced int(11),
sexual_violence_ordeal VARCHAR(255),
physical_violence_experienced int(11),
physical_violence_ordeal VARCHAR(255),
contact_clinical_appointment VARCHAR(10),
contact_method VARCHAR(10),
treatment_supporter_name VARCHAR(255),
treatment_supporter_alias VARCHAR(255),
treatment_supporter_contact VARCHAR(255),
treatment_supporter_alt_contact VARCHAR(255),
enrollment_notes VARCHAR(255),
voided INT(11),
constraint foreign key(client_id) references kp_etl.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
index(client_id),
index(Gender),
index(unique_client_no),
index(enrollment_date),
index(enrollment_service_area)

);
SELECT "Successfully created etl_client_enrollment table";

  -- ------------ create table etl_triage-----------------------
  CREATE TABLE kp_etl.etl_triage (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    weight DOUBLE,
    height DOUBLE,
    systolic_pressure DOUBLE,
    diastolic_pressure DOUBLE,
    temperature DOUBLE,
    pulse_rate DOUBLE,
    respiratory_rate DOUBLE,
    oxygen_saturation DOUBLE,
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(patient_id, visit_date)
  );

  SELECT "Successfully created etl_triage table";


  -- ------------ create table etl_client_complaints-----------------------
  CREATE TABLE kp_etl.etl_client_complaints (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    complaint_exists int(11),
    complaint_type VARCHAR(255),
    nature_of_complaint VARCHAR(255),
    onset_date DATE,
    duration int(11),
    remarks VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(client_id, visit_date)
  );

  SELECT "Successfully created etl_client_complaints table";

  -- ------------ create table etl_chronic_illness-----------------------
  CREATE TABLE kp_etl.etl_chronic_illness (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    illness_exists int(11),
    illness_type VARCHAR(255),
    nature_of_illness VARCHAR(255),
    onset_date DATE,
    duration int(11),
    remarks VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(client_id, visit_date)
  );

  SELECT "Successfully created etl_chronic_illness table";

    -- ------------ create table etl_allergies-----------------------
  CREATE TABLE kp_etl.etl_allergies (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    allergy_exists int(11),
    causative_agent VARCHAR(255),
    reaction VARCHAR(255),
    severity VARCHAR(255),
    onset_date DATE,
    duration int(11),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(client_id, visit_date)
  );

  SELECT "Successfully created etl_allergies table";


-- create table etl_pregnancy_fp_cacx_screening
CREATE TABLE kp_etl.etl_pregnancy_fp_cacx_screening (
uuid char(38),
provider INT(11),
client_id INT(11) NOT NULL,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
lmp DATE,
pregnant INT(11),
edd DATE,
fp_status VARCHAR(255),
elible_for_fp INT(11),
fp_method VARCHAR(255),
referred_for_fp INT(11),
cacx_screening INT(11),
cacx_screening_results INT(11),
treated INT(11),
referred INT(11),
voided INT(11),
CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(client_id),
INDEX(fp_status),
INDEX(elible_for_fp),
INDEX(fp_method),
INDEX(pregnant),
INDEX(cacx_screening),
INDEX(cacx_screening_results),
INDEX(referred_for_fp)
);
SELECT "Successfully created etl_pregnancy_fp_cacx_screening table";

   -- ------------ create table etl_adverse_drug_reaction-----------------------
  CREATE TABLE kp_etl.etl_adverse_drug_reaction (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    adverse_drug_reaction_exists int(11),
    causative_drug VARCHAR(255),
    reaction VARCHAR(255),
    severity VARCHAR(255),
    onset_date DATE,
    action_taken VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(adverse_drug_reaction_exists),
    INDEX(client_id, visit_date)
  );

  SELECT "Successfully created etl_adverse_drug_reaction table";


   -- ------------ create table etl_immunization_screening-----------------------
  CREATE TABLE kp_etl.etl_immunization_screening (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    immunization_done int(11),
    immunization_type VARCHAR(255),
    immunization_date DATE,
    immunization_side_effects int(11),
    nature_of_side_effects VARCHAR(255),
    vaccine_validity int(11),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(immunization_done)
  );

  SELECT "Successfully created etl_immunization_screening table";

 -- ------------ create table etl_sti_screening-----------------------
  CREATE TABLE kp_etl.etl_sti_screening (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    sti_screening_done int(11),
    reason VARCHAR(255),
    provider_name VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(sti_screening_done),
    INDEX(reason)
  );

  SELECT "Successfully created etl_sti_screening table";

   -- ------------ create table etl_hepatitis_screening-----------------------
  CREATE TABLE kp_etl.etl_hepatitis_screening (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    hepatitis_screening_done VARCHAR(100),
    results VARCHAR(255),
    treated INT(11),
    referred INT(11),
    remarks VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(hepatitis_screening_done),
    INDEX(results),
    INDEX(treated)
  );

  SELECT "Successfully created etl_hepatitis_screening table";

 -- ------------ create table etl_tb_screening-----------------------
  CREATE TABLE kp_etl.etl_tb_screening (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    cough_any_duration INT(11),
    fever INT(11),
    noticeable_weight_loss_poor_gain INT(11),
    night_sweats INT(11),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id)
  );

  SELECT "Successfully created etl_tb_screening table";

   -- ------------ create table etl_systems_review-----------------------
  CREATE TABLE kp_etl.etl_systems_review (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    body_systems VARCHAR(255),
    findings VARCHAR(255),
    finding_notes VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(body_systems),
    INDEX(findings)
  );

  SELECT "Successfully created etl_systems_review table";

   -- ------------ create table etl_diagnosis_treatment-----------------------
  CREATE TABLE kp_etl.etl_diagnosis_treatment (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    diagnosis VARCHAR(255),
    treatment_plan VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id)
  );

  SELECT "Successfully created etl_systems_review table";

   -- ------------ create table etl_clinical_notes-----------------------
  CREATE TABLE kp_etl.etl_clinical_notes (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    clinical_notes VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id)
  );
  SELECT "Successfully created etl_clinical_notes table";

-- ------------ create table etl_appointment-----------------------
  CREATE TABLE kp_etl.etl_appointment (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    appointment_date DATETIME,
    appointment_type VARCHAR(255),
    appointment_notes VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(appointment_date),
    INDEX(appointment_type)
  );

  SELECT "Successfully created etl_appointment table";

     -- ------------ create table etl_alcohol_drugs_risk_screening-----------------------
  CREATE TABLE kp_etl.etl_alcohol_drugs_risk_screening (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    screened_for VARCHAR(255),
    results VARCHAR(255),
    treated int(11),
    referred int(11),
    remarks VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(screened_for),
    INDEX(treated)
  );
  SELECT "Successfully created etl_alcohol_drugs_risk_screening table";

       -- ------------ create table etl_violence_screening-----------------------
  CREATE TABLE kp_etl.etl_violence_screening (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    form_of_violence VARCHAR(255),
    place_of_violence VARCHAR(255),
    incident_date DATE,
    target varchar(100),
    perpetrators VARCHAR(255),
    intervention_date DATETIME,
    referral_ordered VARCHAR(255),
    place_of_referral VARCHAR(255),
    referral_date DATETIME,
    outcome_status VARCHAR(255),
    action_plan VARCHAR(255),
    resolution_date DATE,
    program_officer_name VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(form_of_violence),
    INDEX(incident_date),
    INDEX(outcome_status)
  );
  SELECT "Successfully created etl_violence_screening table";

      -- ------------ create table counselling_services-----------------------
  CREATE TABLE kp_etl.counselling_services (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    counselling_type VARCHAR(255),
    referred int(11),
    remarks VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(counselling_type)
  );
  SELECT "Successfully created counselling_services table";

      -- ------------ create table etl_prep_pep_screening-----------------------
  CREATE TABLE kp_etl.etl_prep_pep_screening (
    uuid CHAR(38),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    client_id INT(11) NOT NULL ,
    location_id INT(11) DEFAULT NULL,
    visit_date DATE,
    visit_id INT(11),
    encounter_provider INT(11),
    date_created DATE,
    screened_for VARCHAR(255),
    status VARCHAR(255),
    referred INT(11),
    using_pep INT(11),
    exposure_type VARCHAR(255),
    remarks VARCHAR(255),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(client_id),
    INDEX(screened_for),
    INDEX(status),
    INDEX(using_pep),
    INDEX(exposure_type)
  );
  SELECT "Successfully created etl_prep_pep_screening table";

-- ------------ create table etl_hts_test-----------------------
create table kp_etl.etl_hts_test (
client_id INT(11) not null,
visit_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL primary key,
encounter_uuid CHAR(38) NOT NULL,
encounter_location INT(11) NOT NULL,
creator INT(11) NOT NULL,
date_created DATE NOT NULL,
visit_date DATE,
test_type INT(11) DEFAULT NULL,
population_type VARCHAR(50),
key_population_type VARCHAR(50),
ever_tested_for_hiv VARCHAR(10),
months_since_last_test INT(11),
patient_disabled VARCHAR(50),
disability_type VARCHAR(50),
patient_consented VARCHAR(50) DEFAULT NULL,
client_tested_as VARCHAR(50),
test_strategy VARCHAR(50),
hts_entry_point VARCHAR(50),
test_1_kit_name VARCHAR(50),
test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
test_1_kit_expiry DATE DEFAULT NULL,
test_1_result VARCHAR(50) DEFAULT NULL,
test_2_kit_name VARCHAR(50),
test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
test_2_kit_expiry DATE DEFAULT NULL,
test_2_result VARCHAR(50) DEFAULT NULL,
final_test_result VARCHAR(50) DEFAULT NULL,
patient_given_result VARCHAR(50) DEFAULT NULL,
couple_discordant VARCHAR(100) DEFAULT NULL,
tb_screening VARCHAR(20) DEFAULT NULL,
patient_had_hiv_self_test VARCHAR(50) DEFAULT NULL,
remarks VARCHAR(255) DEFAULT NULL,
voided INT(11),
CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
index(client_id),
index(visit_id),
index(tb_screening),
index(visit_date),
index(population_type),
index(test_type),
index(final_test_result),
index(couple_discordant),
index(test_1_kit_name),
index(test_2_kit_name)
);
SELECT "Successfully created etl_hts_test table";


-- ------------- create etl_hts_referral_and_linkage table ------------------------

CREATE TABLE kp_etl.etl_hts_referral_and_linkage (
client_id INT(11) not null,
visit_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL primary key,
encounter_uuid CHAR(38) NOT NULL,
encounter_location INT(11) NOT NULL,
creator INT(11) NOT NULL,
date_created DATE NOT NULL,
visit_date DATE,
tracing_type VARCHAR(50),
tracing_status VARCHAR(100),
ccc_number VARCHAR(100),
facility_linked_to VARCHAR(100),
enrollment_date DATE,
art_start_date DATE,
provider_handed_to VARCHAR(100),
provider_cadre VARCHAR(100),
remarks VARCHAR(255),
voided INT(11),
CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
index(client_id),
index(visit_date),
index(tracing_type),
index(tracing_status)
);

SELECT "Successfully created etl_hts_referral_and_linkage table";


-- ------------- create etl_client_tracing table ------------------------
CREATE TABLE kp_etl.etl_client_tracing (
uuid char(38),
provider INT(11),
client_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
tracing_attempt_date DATE,
tracing_type INT(11),
tracing_outcome INT(11),
negative_outcome_reason VARCHAR(100),
negative_outcome_description VARCHAR(255),
next_tracing_attempt_date DATE,
final_tracing_status VARCHAR(100),
cause_of_death INT(11),
comments VARCHAR(100),
CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(client_id),
INDEX(final_tracing_status),
INDEX(cause_of_death),
INDEX(tracing_type)
);
SELECT "Successfully created etl_client_tracing table";


-- ------------- create etl_hiv_status table ------------------------
CREATE TABLE kp_etl.etl_hiv_status (
uuid char(38),
provider INT(11),
client_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
ever_tested INT(11),
test_date DATE,
test_results_status VARCHAR(100),
current_in_care INT(11),
referral INT(11),
referred_from VARCHAR(100),
art_start_date DATE,
treatment_facility VARCHAR(100),
current_regimen VARCHAR(100),
recent_vl_result VARCHAR(100),
vl_test_date DATE,
refer_to_hts INT(11),
provider VARCHAR(100),
CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(client_id),
INDEX(ever_tested),
INDEX(test_results_status),
INDEX(treatment_facility)
);
SELECT "Successfully created etl_hiv_status table";


-- create table etl_pregnancy_fp_cacx_screening
CREATE TABLE kp_etl.etl_patient_hiv_followup (
uuid CHAR(38),
encounter_id INT(11) NOT NULL PRIMARY KEY,
patient_id INT(11) NOT NULL ,
location_id INT(11) DEFAULT NULL,
visit_date DATE,
visit_id INT(11),
encounter_provider INT(11),
date_created DATE,
visit_scheduled INT(11),
person_present INT(11),
weight DOUBLE,
systolic_pressure DOUBLE,
diastolic_pressure DOUBLE,
height DOUBLE,
temperature DOUBLE,
pulse_rate DOUBLE,
respiratory_rate DOUBLE,
oxygen_saturation DOUBLE,
muac DOUBLE,
nutritional_status INT(11) DEFAULT NULL,
population_type INT(11) DEFAULT NULL,
key_population_type INT(11) DEFAULT NULL,
who_stage INT(11),
presenting_complaints INT(11) DEFAULT NULL,
clinical_notes VARCHAR(600) DEFAULT NULL,
on_anti_tb_drugs INT(11) DEFAULT NULL,
on_ipt INT(11) DEFAULT NULL,
ever_on_ipt INT(11) DEFAULT NULL,
spatum_smear_ordered INT(11) DEFAULT NULL,
chest_xray_ordered INT(11) DEFAULT NULL,
genexpert_ordered INT(11) DEFAULT NULL,
spatum_smear_result INT(11) DEFAULT NULL,
chest_xray_result INT(11) DEFAULT NULL,
genexpert_result INT(11) DEFAULT NULL,
referral INT(11) DEFAULT NULL,
clinical_tb_diagnosis INT(11) DEFAULT NULL,
contact_invitation INT(11) DEFAULT NULL,
evaluated_for_ipt INT(11) DEFAULT NULL,
has_known_allergies INT(11) DEFAULT NULL,
has_chronic_illnesses_cormobidities INT(11) DEFAULT NULL,
has_adverse_drug_reaction INT(11) DEFAULT NULL,
substitution_first_line_regimen_date DATE ,
substitution_first_line_regimen_reason INT(11),
substitution_second_line_regimen_date DATE,
substitution_second_line_regimen_reason INT(11),
second_line_regimen_change_date DATE,
second_line_regimen_change_reason INT(11),
pregnancy_status INT(11),
wants_pregnancy INT(11) DEFAULT NULL,
pregnancy_outcome INT(11),
anc_number VARCHAR(50),
expected_delivery_date DATE,
last_menstrual_period DATE,
gravida INT(11),
parity INT(11),
full_term_pregnancies INT(11),
abortion_miscarriages INT(11),
family_planning_status INT(11),
family_planning_method INT(11),
reason_not_using_family_planning INT(11),
tb_status INT(11),
tb_treatment_no VARCHAR(50),
ctx_adherence INT(11),
ctx_dispensed INT(11),
dapsone_adherence INT(11),
dapsone_dispensed INT(11),
inh_dispensed INT(11),
arv_adherence INT(11),
poor_arv_adherence_reason INT(11),
poor_arv_adherence_reason_other VARCHAR(200),
pwp_disclosure INT(11),
pwp_partner_tested INT(11),
condom_provided INT(11),
screened_for_sti INT(11),
cacx_screening INT(11), 
sti_partner_notification INT(11),
at_risk_population INT(11),
system_review_finding INT(11),
next_appointment_date DATE,
next_appointment_reason INT(11),
stability INT(11),
differentiated_care INT(11),
voided INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(patient_id, visit_date),
INDEX(who_stage),
INDEX(pregnancy_status),
INDEX(pregnancy_outcome),
INDEX(family_planning_status),
INDEX(family_planning_method),
INDEX(tb_status),
INDEX(condom_provided),
INDEX(ctx_dispensed),
INDEX(inh_dispensed),
INDEX(at_risk_population),
INDEX(population_type),
INDEX(key_population_type),
INDEX(on_anti_tb_drugs),
INDEX(on_ipt),
INDEX(ever_on_ipt),
INDEX(differentiated_care),
INDEX(visit_date, patient_id),
INDEX(visit_date, condom_provided),
INDEX(visit_date, family_planning_method)

);

SELECT "Successfully created etl_patient_hiv_followup table";

-- ------- create table etl_laboratory_extract-----------------------------------------
  SELECT "Creating etl_laboratory_extract table";
CREATE TABLE kp_etl.etl_laboratory_extract (
uuid char(38) PRIMARY KEY,
encounter_id INT(11),
patient_id INT(11) NOT NULL ,
location_id INT(11) DEFAULT NULL,
visit_date DATE,
visit_id INT(11),
lab_test VARCHAR(180),
urgency VARCHAR(50),
test_result VARCHAR(180),
date_test_requested DATE DEFAULT null,
date_test_result_received DATE,
test_requested_by INT(11),
date_created DATE,
created_by INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(lab_test),
INDEX(test_result)

);
SELECT "Successfully created etl_laboratory_extract table";

-- ------------ create table etl_pharmacy_extract-----------------------


CREATE TABLE kp_etl.etl_pharmacy_extract(
obs_group_id INT(11) PRIMARY KEY,
uuid char(38),
patient_id INT(11) NOT NULL ,
location_id INT(11) DEFAULT NULL,
visit_date DATE,
visit_id INT(11),
encounter_id INT(11),
encounter_name VARCHAR(100),
drug INT(11),
is_arv INT(11),
is_ctx INT(11),
is_dapsone INT(11),
drug_name VARCHAR(255),
dose INT(11),
unit INT(11),
frequency INT(11),
duration INT(11),
duration_units VARCHAR(20) ,
duration_in_days INT(11),
prescription_provider VARCHAR(50),
dispensing_provider VARCHAR(50),
regimen MEDIUMTEXT,
adverse_effects VARCHAR(100),
date_of_refill DATE,
date_created DATE,
voided INT(11),
date_voided DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(drug),
INDEX(is_arv)

);
SELECT "Successfully created etl_pharmacy_extract table";
-- ------------ create table etl_patient_treatment_discontinuation-----------------------

CREATE TABLE kp_etl.etl_patient_program_discontinuation(
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATETIME,
location_id INT(11) DEFAULT NULL,
program_uuid CHAR(38) ,
program_name VARCHAR(50),
encounter_id INT(11) NOT NULL PRIMARY KEY,
discontinuation_reason INT(11),
date_died DATE,
transfer_facility VARCHAR(100),
transfer_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(visit_date, program_name, patient_id),
INDEX(visit_date, patient_id),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(discontinuation_reason),
INDEX(date_died),
INDEX(transfer_date)
);
SELECT "Successfully created etl_patient_program_discontinuation table";

-- ------------ create table etl_mch_enrollment-----------------------
  CREATE TABLE kp_etl.etl_mch_enrollment (
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    anc_number VARCHAR(50),
    first_anc_visit_date DATE,
    gravida INT(11),
    parity INT(11),
    parity_abortion INT(11),
    age_at_menarche INT(11),
    lmp DATE,
    lmp_estimated INT(11),
    edd_ultrasound DATE,
    blood_group INT(11),
    serology INT(11),
    tb_screening INT(11),
    bs_for_mps INT(11),
    hiv_status INT(11),
    hiv_test_date DATE,
    partner_hiv_status INT(11),
    partner_hiv_test_date DATE,
    urine_microscopy VARCHAR(100),
    urinary_albumin INT(11),
    glucose_measurement INT(11),
    urine_ph INT(11),
    urine_gravity INT(11),
    urine_nitrite_test INT(11),
    urine_leukocyte_esterace_test INT(11),
    urinary_ketone INT(11),
    urine_bile_salt_test INT(11),
    urine_bile_pigment_test INT(11),
    urine_colour INT(11),
    urine_turbidity INT(11),
    urine_dipstick_for_blood INT(11),
    date_of_discontinuation DATETIME,
    discontinuation_reason INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(tb_screening),
    INDEX(hiv_status),
    INDEX(hiv_test_date),
    INDEX(partner_hiv_status)
  );
  SELECT "Successfully created etl_mch_enrollment table";

  -- ------------ create table etl_enrollment-----------------------

  CREATE TABLE kp_etl.etl_mch_antenatal_visit (
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    provider INT(11),
    anc_visit_number INT(11),
    temperature DOUBLE,
    pulse_rate DOUBLE,
    systolic_bp DOUBLE,
    diastolic_bp DOUBLE,
    respiratory_rate DOUBLE,
    oxygen_saturation INT(11),
    weight DOUBLE,
    height DOUBLE,
    muac DOUBLE,
    hemoglobin DOUBLE,
    breast_exam_done INT(11),
    pallor INT(11),
    maturity INT(11),
    fundal_height DOUBLE,
    fetal_presentation INT(11),
    lie INT(11),
    fetal_heart_rate INT(11),
    fetal_movement INT(11),
    who_stage INT(11),
    cd4 INT(11),
    viral_load INT(11),
    ldl INT(11),
    arv_status INT(11),
    test_1_kit_name VARCHAR(50),
    test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_1_kit_expiry DATE DEFAULT NULL,
    test_1_result VARCHAR(50) DEFAULT NULL,
    test_2_kit_name VARCHAR(50),
    test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_2_kit_expiry DATE DEFAULT NULL,
    test_2_result VARCHAR(50) DEFAULT NULL,
    final_test_result VARCHAR(50) DEFAULT NULL,
    patient_given_result VARCHAR(50) DEFAULT NULL,
    partner_hiv_tested INT(11),
    partner_hiv_status INT(11),
    prophylaxis_given INT(11),
    baby_azt_dispensed INT(11),
    baby_nvp_dispensed INT(11),
    TTT VARCHAR(50) DEFAULT NULL,
    IPT_malaria VARCHAR(50) DEFAULT NULL,
    iron_supplement VARCHAR(50) DEFAULT NULL,
    deworming VARCHAR(50) DEFAULT NULL,
    bed_nets VARCHAR(50) DEFAULT NULL,
    urine_microscopy VARCHAR(100),
    urinary_albumin INT(11),
    glucose_measurement INT(11),
    urine_ph INT(11),
    urine_gravity INT(11),
    urine_nitrite_test INT(11),
    urine_leukocyte_esterace_test INT(11),
    urinary_ketone INT(11),
    urine_bile_salt_test INT(11),
    urine_bile_pigment_test INT(11),
    urine_colour INT(11),
    urine_turbidity INT(11),
    urine_dipstick_for_blood INT(11),
    syphilis_test_status INT(11),
    syphilis_treated_status INT(11),
    bs_mps INT(11),
    anc_exercises INT(11),
    tb_screening INT(11),
    cacx_screening INT(11),
    cacx_screening_method INT(11),
    has_other_illnes INT(11),
    counselled INT(11),
    referred_from INT(11),
    referred_to INT(11),
    next_appointment_date DATE,
    clinical_notes VARCHAR(200) DEFAULT NULL,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(who_stage),
    INDEX(anc_visit_number),
    INDEX(final_test_result),
    INDEX(tb_screening),
    INDEX(syphilis_test_status),
    INDEX(cacx_screening),
    INDEX(next_appointment_date),
    INDEX(arv_status)
  );
  SELECT "Successfully created etl_mch_antenatal_visit table";

  -- ------------ create table etl_mchs_delivery-----------------------

  CREATE TABLE kp_etl.etl_mchs_delivery (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    admission_number VARCHAR(50),
    duration_of_pregnancy DOUBLE,
    mode_of_delivery INT(11),
    date_of_delivery DATETIME,
    blood_loss INT(11),
    condition_of_mother INT(11),
    apgar_score_1min  DOUBLE,
    apgar_score_5min  DOUBLE,
    apgar_score_10min DOUBLE,
    resuscitation_done INT(11),
    place_of_delivery INT(11),
    delivery_assistant VARCHAR(100),
    counseling_on_infant_feeding  INT(11),
    counseling_on_exclusive_breastfeeding INT(11),
    counseling_on_infant_feeding_for_hiv_infected INT(11),
    mother_decision INT(11),
    placenta_complete INT(11),
    maternal_death_audited INT(11),
    cadre INT(11),
    delivery_complications INT(11),
    coded_delivery_complications INT(11),
    other_delivery_complications VARCHAR(100),
    duration_of_labor INT(11),
    baby_sex INT(11),
    baby_condition INT(11),
    teo_given INT(11),
    birth_weight INT(11),
    bf_within_one_hour INT(11),
    birth_with_deformity INT(11),
    test_1_kit_name VARCHAR(50),
    test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_1_kit_expiry DATE DEFAULT NULL,
    test_1_result VARCHAR(50) DEFAULT NULL,
    test_2_kit_name VARCHAR(50),
    test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_2_kit_expiry DATE DEFAULT NULL,
    test_2_result VARCHAR(50) DEFAULT NULL,
    final_test_result VARCHAR(50) DEFAULT NULL,
    patient_given_result VARCHAR(50) DEFAULT NULL,
    partner_hiv_tested INT(11),
    partner_hiv_status INT(11),
    prophylaxis_given INT(11),
    baby_azt_dispensed INT(11),
    baby_nvp_dispensed INT(11),
    clinical_notes VARCHAR(200) DEFAULT NULL,

    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(final_test_result),
    INDEX(baby_sex),
    INDEX( partner_hiv_tested),
    INDEX( partner_hiv_status)

  );
  SELECT "Successfully created etl_mchs_delivery table";

  -- ------------ create table etl_mchs_discharge-----------------------

  CREATE TABLE kp_etl.etl_mchs_discharge (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    date_created DATE,
    counselled_on_feeding INT(11),
    baby_status INT(11),
    vitamin_A_dispensed INT(11),
    birth_notification_number INT(50),
    condition_of_mother VARCHAR(100),
    discharge_date DATE,
    referred_from INT(11),
    referred_to INT(11),
    clinical_notes VARCHAR(200) DEFAULT NULL,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(baby_status),
    INDEX(discharge_date)
  );
  SELECT "Successfully created etl_mchs_discharge table";

  -- ------------ create table etl_mch_postnatal_visit-----------------------

  CREATE TABLE kp_etl.etl_mch_postnatal_visit (
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    provider INT(11),
    pnc_register_no VARCHAR(50),
    pnc_visit_no INT(11),
    delivery_date DATE,
    mode_of_delivery INT(11),
    place_of_delivery INT(11),
    temperature DOUBLE,
    pulse_rate DOUBLE,
    systolic_bp DOUBLE,
    diastolic_bp DOUBLE,
    respiratory_rate DOUBLE,
    oxygen_saturation INT(11),
    weight DOUBLE,
    height DOUBLE,
    muac DOUBLE,
    hemoglobin DOUBLE,
    arv_status INT(11),
    general_condition INT(11),
    breast INT(11),
    cs_scar INT(11),
    gravid_uterus INT(11),
    episiotomy INT(11),
    lochia INT(11),
    pallor INT(11),
    pph INT(11),
    mother_hiv_status INT(11),
    condition_of_baby INT(11),
    baby_feeding_method INT(11),
    umblical_cord INT(11),
    baby_immunization_started INT(11),
    family_planning_counseling INT(11),
    uterus_examination VARCHAR(100),
    uterus_cervix_examination VARCHAR(100),
    vaginal_examination VARCHAR(100),
    parametrial_examination VARCHAR(100),
    external_genitalia_examination VARCHAR(100),
    ovarian_examination VARCHAR(100),
    pelvic_lymph_node_exam VARCHAR(100),
    test_1_kit_name VARCHAR(50),
    test_1_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_1_kit_expiry DATE DEFAULT NULL,
    test_1_result VARCHAR(50) DEFAULT NULL,
    test_2_kit_name VARCHAR(50),
    test_2_kit_lot_no VARCHAR(50) DEFAULT NULL,
    test_2_kit_expiry DATE DEFAULT NULL,
    test_2_result VARCHAR(50) DEFAULT NULL,
    final_test_result VARCHAR(50) DEFAULT NULL,
    patient_given_result VARCHAR(50) DEFAULT NULL,
    partner_hiv_tested INT(11),
    partner_hiv_status INT(11),
    prophylaxis_given INT(11),
    baby_azt_dispensed INT(11),
    baby_nvp_dispensed INT(11),
    pnc_exercises INT(11),
    maternal_condition INT(11),
    iron_supplementation INT(11),
    fistula_screening INT(11),
    cacx_screening INT(11),
    cacx_screening_method INT(11),
    family_planning_status INT(11),
    family_planning_method INT(11),
    referred_from INT(11),
    referred_to INT(11),
    clinical_notes VARCHAR(200) DEFAULT NULL,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(arv_status),
    INDEX(mother_hiv_status),
    INDEX(arv_status)
  );

  SELECT "Successfully created etl_mch_postnatal_visit table";
  -- ------------ create table etl_hei_enrollment-----------------------

  CREATE TABLE kp_etl.etl_hei_enrollment (
    serial_no INT(11)NOT NULL AUTO_INCREMENT,
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    provider INT(11),
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    child_exposed INT(11),
    hei_id_number VARCHAR(50),
    spd_number VARCHAR(50),
    birth_weight DOUBLE,
    gestation_at_birth DOUBLE,
    date_first_seen DATE,
    birth_notification_number VARCHAR(50),
    birth_certificate_number VARCHAR(50),
    need_for_special_care INT(11),
    reason_for_special_care INT(11),
    referral_source INT(11),
    transfer_in INT(11),
    transfer_in_date DATE,
    facility_transferred_from VARCHAR(50),
    district_transferred_from VARCHAR(50),
    date_first_enrolled_in_hei_care DATE,
    arv_prophylaxis INT(11),
    mother_breastfeeding INT(11),
    mother_on_NVP_during_breastfeeding INT(11),
    TB_contact_history_in_household INT(11),
    infant_mother_link INT(11),
    mother_alive INT(11),
    mother_on_pmtct_drugs INT(11),
    mother_on_drug INT(11),
    mother_on_art_at_infant_enrollment INT(11),
    mother_drug_regimen INT(11),
    infant_prophylaxis INT(11),
    parent_ccc_number VARCHAR(50),
    mode_of_delivery INT(11),
    place_of_delivery INT(11),
    birth_length INT(11),
    birth_order INT(11),
    health_facility_name VARCHAR(50),
    date_of_birth_notification DATE,
    date_of_birth_registration DATE,
    birth_registration_place VARCHAR(50),
    permanent_registration_serial VARCHAR(50),
    mother_facility_registered VARCHAR(50),
    exit_date DATE,
    exit_reason INT(11),
    hiv_status_at_exit VARCHAR(50),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(patient_id),
    INDEX(transfer_in),
    INDEX(child_exposed),
    INDEX(need_for_special_care),
    INDEX(reason_for_special_care),
    INDEX(referral_source),
    INDEX(transfer_in),
    INDEX(serial_no)
  );
  SELECT "Successfully created etl_hei_enrollment table";

  -- ------------ create table etl_hei_follow_up_visit-----------------------

  CREATE TABLE kp_etl.etl_hei_follow_up_visit (
    uuid char(38),
    provider INT(11),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    weight DOUBLE,
    height DOUBLE,
    primary_caregiver INT(11),
    infant_feeding INT(11),
    tb_assessment_outcome INT(11),
    social_smile_milestone INT(11),
    head_control_milestone INT(11),
    response_to_sound_milestone INT(11),
    hand_extension_milestone INT(11),
    sitting_milestone INT(11),
    walking_milestone INT(11),
    standing_milestone INT(11),
    talking_milestone INT(11),
    review_of_systems_developmental INT(11),
    dna_pcr_sample_date DATE,
    dna_pcr_contextual_status INT(11),
    dna_pcr_result INT(11),
    dna_pcr_dbs_sample_code VARCHAR(100),
    dna_pcr_results_date DATE,
    azt_given INT(11),
    nvp_given INT(11),
    ctx_given INT(11),
    first_antibody_sample_date DATE,
    first_antibody_result INT(11),
    first_antibody_dbs_sample_code VARCHAR(100),
    first_antibody_result_date DATE,
    final_antibody_sample_date DATE,
    final_antibody_result INT(11),
    final_antibody_dbs_sample_code VARCHAR(100),
    final_antibody_result_date DATE,
    tetracycline_ointment_given INT(11),
    pupil_examination INT(11),
    sight_examination INT(11),
    squint INT(11),
    deworming_drug INT(11),
    dosage INT(11),
    unit VARCHAR(100),
    next_appointment_date DATE,
    comments VARCHAR(100),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id),
    INDEX(infant_feeding)
  );
  SELECT "Successfully created etl_hei_follow_up_visit table";

  -- ------- create table etl_hei_immunization table-----------------------------------------
  SELECT "Creating etl_hei_immunization table";
  CREATE TABLE kp_etl.etl_hei_immunization (
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    patient_id INT(11) NOT NULL ,
    visit_date DATE,
    date_created DATE,
    created_by INT(11),
    BCG VARCHAR(50),
    OPV_birth VARCHAR(50),
    OPV_1 VARCHAR(50),
    OPV_2 VARCHAR(50),
    OPV_3 VARCHAR(50),
    IPV VARCHAR(50),
    DPT_Hep_B_Hib_1 VARCHAR(50),
    DPT_Hep_B_Hib_2 VARCHAR(50),
    DPT_Hep_B_Hib_3 VARCHAR(50),
    PCV_10_1 VARCHAR(50),
    PCV_10_2 VARCHAR(50),
    PCV_10_3 VARCHAR(50),
    ROTA_1 VARCHAR(50),
    ROTA_2 VARCHAR(50),
    Measles_rubella_1 VARCHAR(50),
    Measles_rubella_2 VARCHAR(50),
    Yellow_fever VARCHAR(50),
    Measles_6_months VARCHAR(50),
    VitaminA_6_months VARCHAR(50),
    VitaminA_1_yr VARCHAR(50),
    VitaminA_1_and_half_yr VARCHAR(50),
    VitaminA_2_yr VARCHAR(50),
    VitaminA_2_to_5_yr VARCHAR(50),
    fully_immunized DATE,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    INDEX(visit_date),
    INDEX(encounter_id)


  );
  SELECT "Successfully created etl_hei_immunization table";

-- ------------ create table etl_tb_enrollment-----------------------

CREATE TABLE kp_etl.etl_tb_enrollment (
uuid char(38),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
provider INT(11),
date_treatment_started DATE,
district VARCHAR(50),
district_registration_number VARCHAR(20),
referred_by INT(11),
referral_date DATE,
date_transferred_in DATE,
facility_transferred_from VARCHAR(100),
district_transferred_from VARCHAR(100),
date_first_enrolled_in_tb_care DATE,
weight DOUBLE,
height DOUBLE,
treatment_supporter VARCHAR(100),
relation_to_patient INT(11),
treatment_supporter_address VARCHAR(100),
treatment_supporter_phone_contact VARCHAR(100),
disease_classification INT(11),
patient_classification INT(11),
pulmonary_smear_result INT(11),
has_extra_pulmonary_pleurial_effusion INT(11),
has_extra_pulmonary_milliary INT(11),
has_extra_pulmonary_lymph_node INT(11),
has_extra_pulmonary_menengitis INT(11),
has_extra_pulmonary_skeleton INT(11),
has_extra_pulmonary_abdominal INT(11),
has_extra_pulmonary_other VARCHAR(100),
treatment_outcome INT(11),
treatment_outcome_date DATE,
date_of_discontinuation DATETIME,
discontinuation_reason INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(disease_classification),
INDEX(patient_classification),
INDEX(pulmonary_smear_result),
INDEX(date_first_enrolled_in_tb_care)
);

-- ------------ create table etl_tb_follow_up_visit-----------------------

CREATE TABLE kp_etl.etl_tb_follow_up_visit (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
spatum_test INT(11),
spatum_result INT(11),
result_serial_number VARCHAR(20),
quantity DOUBLE ,
date_test_done DATE,
bacterial_colonie_growth INT(11),
number_of_colonies DOUBLE,
resistant_s INT(11),
resistant_r INT(11),
resistant_inh INT(11),
resistant_e INT(11),
sensitive_s INT(11),
sensitive_r INT(11),
sensitive_inh INT(11),
sensitive_e INT(11),
test_date DATE,
hiv_status INT(11),
next_appointment_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(hiv_status)
);

-- ------------ create table etl_tb_screening-----------------------

CREATE TABLE kp_etl.etl_tb_screening (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
cough_for_2wks_or_more INT(11),
confirmed_tb_contact INT(11),
fever_for_2wks_or_more INT(11),
noticeable_weight_loss INT(11),
night_sweat_for_2wks_or_more INT(11),
resulting_tb_status INT(11),
tb_treatment_start_date DATE DEFAULT NULL,
notes VARCHAR(100),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(cough_for_2wks_or_more),
INDEX(confirmed_tb_contact),
INDEX(noticeable_weight_loss),
INDEX(night_sweat_for_2wks_or_more),
INDEX(resulting_tb_status)
);

-- ------------ create table etl_patients_booked_today-----------------------

CREATE TABLE kp_etl.etl_patients_booked_today(
id INT(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
patient_id INT(11) NOT NULL ,
last_visit_date DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
INDEX(patient_id)
);

-- ------------ create table etl_missed_appointments-----------------------

CREATE TABLE kp_etl.etl_missed_appointments(
id INT(11) NOT NULL PRIMARY KEY,
patient_id INT(11) NOT NULL ,
last_tca_date DATE,
last_visit_date DATE,
last_encounter_type VARCHAR(100),
days_since_last_visit INT(11),
date_table_created DATE,
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
INDEX(patient_id)
);

-- --------------------------- CREATE drug_event table ---------------------

  CREATE TABLE kp_etl.etl_drug_event(
    uuid CHAR(38) PRIMARY KEY,
    patient_id INT(11) NOT NULL,
    date_started DATE,
    visit_date DATE,
    provider INT(11),
    encounter_id INT(11) NOT NULL,
    program VARCHAR(50),
    regimen MEDIUMTEXT,
    regimen_name VARCHAR(100),
    regimen_line VARCHAR(50),
    discontinued INT(11),
    regimen_discontinued VARCHAR(255),
    date_discontinued DATE,
    reason_discontinued INT(11),
    reason_discontinued_other VARCHAR(100),
    voided INT(11),
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    INDEX(patient_id),
    INDEX(date_started),
    INDEX(date_discontinued),
    INDEX(patient_id, date_started)
  );

-- ------------ create table etl_ipt_screening-----------------------

CREATE TABLE kp_etl.etl_ipt_screening (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
yellow_urine INT(11),
numbness INT(11),
yellow_eyes INT(11),
abdominal_tenderness INT(11),
ipt_started INT(11),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(patient_id),
INDEX(visit_date, ipt_started, patient_id),
INDEX(ipt_started, visit_date),
INDEX(encounter_id),
INDEX(ipt_started)
);

-- ------------ create table etl_ipt_follow_up -----------------------

CREATE TABLE kp_etl.etl_ipt_follow_up (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
ipt_due_date DATE DEFAULT NULL,
date_collected_ipt DATE DEFAULT NULL,
hepatotoxity VARCHAR(100) DEFAULT NULL,
peripheral_neuropathy VARCHAR(100) DEFAULT NULL ,
rash VARCHAR(100),
adherence VARCHAR(100),
outcome VARCHAR(100),
date_of_outcome DATE DEFAULT NULL,
discontinuation_reason VARCHAR(100),
action_taken VARCHAR(100),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(visit_date, discontinuation_reason),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(ipt_due_date),
INDEX(date_collected_ipt),
INDEX(hepatotoxity),
INDEX(peripheral_neuropathy),
INDEX(rash),
INDEX(adherence),
INDEX(outcome),
INDEX(discontinuation_reason)
);

CREATE TABLE kp_etl.etl_ccc_defaulter_tracing (
uuid char(38),
provider INT(11),
patient_id INT(11) NOT NULL ,
visit_id INT(11),
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL PRIMARY KEY,
tracing_type INT(11),
tracing_outcome INT(11),
attempt_number INT(11),
is_final_trace INT(11) ,
true_status INT(11),
cause_of_death INT(11),
comments VARCHAR(100),
CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(patient_id),
INDEX(true_status),
INDEX(cause_of_death),
INDEX(tracing_type)
);

-- ------------ create table etl_ART_preparation-----------------------
CREATE TABLE kp_etl.etl_ART_preparation (
  uuid char(38),
  patient_id INT(11) NOT NULL ,
  visit_id INT(11),
  visit_date DATE,
  location_id INT(11) DEFAULT NULL,
  encounter_id INT(11) NOT NULL PRIMARY KEY,
  provider INT(11),
  understands_hiv_art_benefits varchar(10),
  screened_negative_substance_abuse varchar(10),
  screened_negative_psychiatric_illness varchar(10),
  HIV_status_disclosure varchar(10),
  trained_drug_admin varchar(10),
  informed_drug_side_effects varchar(10),
  caregiver_committed varchar(10),
  adherance_barriers_identified varchar(10),
  caregiver_location_contacts_known varchar(10),
  ready_to_start_art varchar(10),
  identified_drug_time varchar(10),
  treatment_supporter_engaged varchar(10),
  support_grp_meeting_awareness varchar(10),
  enrolled_in_reminder_system varchar(10),
  other_support_systems varchar(10),
  CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
  CONSTRAINT unique_uuid UNIQUE(uuid),
  INDEX(visit_date),
  INDEX(encounter_id),
  INDEX(ready_to_start_art)
);
SELECT "Successfully created etl_ART_preparation table";

  -- ------------ create table etl_enhanced_adherence-----------------------
  CREATE TABLE kp_etl.etl_enhanced_adherence (
    uuid char(38),
    patient_id INT(11) NOT NULL ,
    visit_id INT(11),
    visit_date DATE,
    location_id INT(11) DEFAULT NULL,
    encounter_id INT(11) NOT NULL PRIMARY KEY,
    provider INT(11),
    session_number INT(11),
    first_session_date DATE,
    pill_count INT(11),
    arv_adherence varchar(50),
    has_vl_results varchar(10),
    vl_results_suppressed varchar(10),
    vl_results_feeling varchar(255),
    cause_of_high_vl varchar(255),
    way_forward varchar(255),
    patient_hiv_knowledge varchar(255),
    patient_drugs_uptake varchar(255),
    patient_drugs_reminder_tools varchar(255),
    patient_drugs_uptake_during_travels varchar(255),
    patient_drugs_side_effects_response varchar(255),
    patient_drugs_uptake_most_difficult_times varchar(255),
    patient_drugs_daily_uptake_feeling varchar(255),
    patient_ambitions varchar(255),
    patient_has_people_to_talk varchar(10),
    patient_enlisting_social_support varchar(255),
    patient_income_sources varchar(255),
    patient_challenges_reaching_clinic varchar(10),
    patient_worried_of_accidental_disclosure varchar(10),
    patient_treated_differently varchar(10),
    stigma_hinders_adherence varchar(10),
    patient_tried_faith_healing varchar(10),
    patient_adherence_improved varchar(10),
    patient_doses_missed varchar(10),
    review_and_barriers_to_adherence varchar(255),
    other_referrals varchar(10),
    appointments_honoured varchar(10),
    referral_experience varchar(255),
    home_visit_benefit varchar(10),
    adherence_plan varchar(255),
    next_appointment_date DATE,
    CONSTRAINT FOREIGN KEY (patient_id) REFERENCES kp_etl.etl_patient_demographics(patient_id),
    CONSTRAINT unique_uuid UNIQUE(uuid),
    INDEX(visit_date),
    INDEX(encounter_id)
    );
  SELECT "Successfully created etl_enhanced_adherence table";



  UPDATE kp_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$





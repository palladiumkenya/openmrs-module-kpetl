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
DROP TABLE IF EXISTS kp_etl.etl_triage;
DROP TABLE IF EXISTS kp_etl.etl_client_complaints;
DROP TABLE IF EXISTS kp_etl.etl_chronic_illness;
DROP TABLE IF EXISTS kp_etl.etl_allergies;
DROP TABLE IF EXISTS kp_etl.etl_pregnancy_fp_cacx_screening;
DROP TABLE IF EXISTS kp_etl.etl_adverse_drug_reaction;
DROP TABLE IF EXISTS kp_etl.etl_immunization_screening;
DROP TABLE IF EXISTS kp_etl.etl_sti_screening;
DROP TABLE IF EXISTS kp_etl.etl_hepatitis_screening;
DROP TABLE IF EXISTS kp_etl.etl_hepatitis_screening;
DROP TABLE IF EXISTS kp_etl.etl_tb_screening;
DROP TABLE IF EXISTS kp_etl.etl_systems_review;
DROP TABLE IF EXISTS kp_etl.etl_diagnosis_treatment;
DROP TABLE IF EXISTS kp_etl.etl_clinical_notes;
DROP TABLE IF EXISTS kp_etl.etl_alcohol_drugs_risk_screening;
DROP TABLE IF EXISTS kp_etl.etl_alcohol_drugs_risk_screening;
DROP TABLE IF EXISTS kp_etl.etl_violence_screening;
DROP TABLE IF EXISTS kp_etl.etl_counselling_services;
DROP TABLE IF EXISTS kp_etl.etl_prep_pep_screening;
DROP TABLE IF EXISTS kp_etl.etl_hts_test;
DROP TABLE IF EXISTS kp_etl.etl_hts_referral_and_linkage;
DROP TABLE IF EXISTS kp_etl.etl_client_tracing;
DROP TABLE IF EXISTS kp_etl.etl_hiv_status;

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
index(client_id),
index(unique_patient_no),
index(Gender),
index(registration_date),
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
date_created DATE,
key_population_type VARCHAR(255),
peer_educator int(11),
hot_spot VARCHAR(255),
weekly_sex_acts int (11),
weekly_anal_sex_acts int (11),
daily_drug_injections int (11),
avg_weekly_drug_injections int (11),
voided INT(11),
constraint foreign key(client_id) references kp_etl.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
index(client_id),
index(key_population_type)
);

SELECT "Successfully created etl_client_social_status table";

-- create table etl_client_enrollment

create table kp_etl.etl_client_enrollment (
uuid char(38) ,
unique_client_no VARCHAR(50),
client_id INT(11) NOT NULL,
visit_id INT(11) DEFAULT NULL,
visit_date DATE,
location_id INT(11) DEFAULT NULL,
encounter_id INT(11) NOT NULL,
encounter_provider INT(11),
date_created DATE,
enrollment_service_area VARCHAR(255),
contacted_for_prevention VARCHAR(10),
contact_person_name VARCHAR(255),
regular_free_sexual_partner VARCHAR(10),
free_sexual_partner_name VARCHAR(255),
free_sexual_partner_alias VARCHAR(255),
free_sexual_partner_contact VARCHAR(255),
contact_regular_sexual_partner VARCHAR(10),
sex_work_startdate DATE,
sex_with_men_startdate DATE,
drug_startdate DATE,
sexual_violence_experienced VARCHAR(10),
sexual_violence_ordeal VARCHAR(255),
physical_violence_experienced VARCHAR(10),
physical_violence_ordeal VARCHAR(255),
contact_clinical_appointment VARCHAR(10),
contact_method VARCHAR(100),
treatment_supporter_name VARCHAR(255),
treatment_supporter_alias VARCHAR(255),
treatment_supporter_contact VARCHAR(255),
treatment_supporter_alt_contact VARCHAR(255),
enrollment_notes VARCHAR(255),
voided INT(11),
constraint foreign key(client_id) references kp_etl.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
index(client_id),
index(unique_client_no),
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
    INDEX(client_id),
    INDEX(client_id, visit_date)
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
    complaint_exists VARCHAR(10),
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
    illness_exists VARCHAR(10),
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
drop table etl_allergies;
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
    allergy_exists VARCHAR(10),
    causative_agent VARCHAR(255),
    reaction VARCHAR(255),
    severity VARCHAR(255),
    onset_date DATE,
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
uuid CHAR(38),
encounter_id INT(11) NOT NULL PRIMARY KEY,
client_id INT(11) NOT NULL ,
location_id INT(11) DEFAULT NULL,
visit_date DATE,
visit_id INT(11),
encounter_provider INT(11),
date_created DATE,
lmp DATE,
pregnant VARCHAR(10),
edd DATE,
fp_status VARCHAR(255),
elible_for_fp VARCHAR(10),
fp_method VARCHAR(255),
referred_for_fp VARCHAR(10),
cacx_screening VARCHAR(10),
cacx_screening_results VARCHAR(10),
treated VARCHAR(10),
referred VARCHAR(10),
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
    adverse_drug_reaction_exists VARCHAR(10),
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
    immunization_done VARCHAR(10),
    immunization_type VARCHAR(255),
    immunization_date DATE,
    immunization_side_effects VARCHAR(10),
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
uuid CHAR(38),
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
uuid CHAR(38),
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
date_created DATE,
tracing_attempt_date DATE,
tracing_type INT(11),
tracing_outcome INT(11),
negative_outcome_reason VARCHAR(100),
negative_outcome_description VARCHAR(255),
next_tracing_attempt_date DATE,
final_tracing_status VARCHAR(100),
cause_of_death INT(11),
comments VARCHAR(100),
CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
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
date_created DATE,
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
CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
CONSTRAINT unique_uuid UNIQUE(uuid),
INDEX(visit_date),
INDEX(encounter_id),
INDEX(client_id),
INDEX(ever_tested),
INDEX(test_results_status),
INDEX(treatment_facility)
);
SELECT "Successfully created etl_hiv_status table";


  UPDATE kp_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$


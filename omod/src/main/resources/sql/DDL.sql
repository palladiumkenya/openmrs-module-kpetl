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
    DROP TABLE IF EXISTS kp_etl.etl_contact;
    DROP TABLE IF EXISTS kp_etl.etl_client_enrollment;
    DROP TABLE IF EXISTS kp_etl.etl_clinical_visit;
    DROP TABLE IF EXISTS kp_etl.etl_peer_calendar;
    DROP TABLE IF EXISTS kp_etl.etl_sti_Treatment;
    /*
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
    DROP TABLE IF EXISTS kp_etl.etl_hiv_status;*/

    -- create table etl_client_registration
    create table kp_etl.etl_client_registration (
      client_id INT(11) not null primary key,
      registration_date DATE,
      given_name VARCHAR(255),
      middle_name VARCHAR(255),
      family_name VARCHAR(255),
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
      index(Gender),
      index(registration_date),
      index(DOB)
    );

    SELECT "Successfully created etl_client_registration table";

    -- create table etl_contact
    create table kp_etl.etl_contact (
      uuid char(38) ,
      unique_identifier VARCHAR(50),
      client_id INT(11) NOT NULL,
      visit_id INT(11) DEFAULT NULL,
      visit_date DATE,
      location_id INT(11) DEFAULT NULL,
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      encounter_provider INT(11),
      date_created DATE,
      key_population_type VARCHAR(255),
      contacted_by_peducator VARCHAR(10),
      program_name VARCHAR(255),
      frequent_hotspot_name VARCHAR(255),
      frequent_hotspot_type VARCHAR(255),
      year_started_sex_work VARCHAR(10),
      year_started_sex_with_men VARCHAR(10),
      year_started_drugs VARCHAR(10),
      avg_weekly_sex_acts int(11),
      avg_weekly_anal_sex_acts int(11),
      avg_weekly_drug_injections int(11),
      contact_person_name VARCHAR(255),
      contact_person_alias VARCHAR(255),
      contact_person_phone VARCHAR(255),
      voided INT(11),
      constraint foreign key(client_id) references kp_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      index(client_id),
      index(unique_identifier),
      index(key_population_type)
    );

    SELECT "Successfully created etl_contact table";

    -- create table etl_client_enrollment

    create table kp_etl.etl_client_enrollment (
      uuid char(38) ,
      client_id INT(11) NOT NULL,
      visit_id INT(11) DEFAULT NULL,
      visit_date DATE,
      location_id INT(11) DEFAULT NULL,
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      encounter_provider INT(11),
      date_created DATE,
      contacted_for_prevention VARCHAR(10),
      has_regular_free_sex_partner VARCHAR(10),
      year_started_sex_work VARCHAR(10),
      year_started_sex_with_men VARCHAR(10),
      year_started_drugs VARCHAR(10),
      has_expereienced_sexual_violence VARCHAR(10),
      has_expereienced_physical_violence VARCHAR(10),
      ever_tested_for_hiv VARCHAR(10),
      test_type VARCHAR(255),
      share_test_results VARCHAR(100),
      willing_to_test VARCHAR(10),
      test_decline_reason VARCHAR(255),
      receiving_hiv_care VARCHAR(10),
      care_facility_name VARCHAR(100),
      ccc_number VARCHAR(100),
      vl_test_done VARCHAR(10),
      vl_results_date DATE,
      contact_for_appointment VARCHAR(10),
      contact_method VARCHAR(255),
      buddy_name VARCHAR(255),
      buddy_phone_number VARCHAR(255),
      voided INT(11),
      constraint foreign key(client_id) references kp_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      index(client_id)
    );
    SELECT "Successfully created etl_client_enrollment table";

    -- create table etl_clinical_visit

    create table kp_etl.etl_clinical_visit (
      uuid char(38) ,
      client_id INT(11) NOT NULL,
      visit_id INT(11) DEFAULT NULL,
      visit_date DATE,
      location_id INT(11) DEFAULT NULL,
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      encounter_provider INT(11),
      date_created DATE,
      sti_screened VARCHAR(10),
      sti_results VARCHAR(255),
      sti_treated VARCHAR(10),
      sti_referred VARCHAR(10),
      sti_referred_text VARCHAR(255),
      tb_screened VARCHAR(10),
      tb_results VARCHAR(255),
      tb_treated VARCHAR(10),
      tb_referred VARCHAR(10),
      tb_referred_text VARCHAR(255),
      hepatitisB_screened VARCHAR(10),
      hepatitisB_results VARCHAR(255),
      hepatitisB_treated VARCHAR(10),
      hepatitisB_referred VARCHAR(10),
      hepatitisB_text VARCHAR(255),
      hepatitisC_screened VARCHAR(10),
      hepatitisC_results VARCHAR(255),
      hepatitisC_treated VARCHAR(10),
      hepatitisC_referred VARCHAR(10),
      hepatitisC_text VARCHAR(255),
      overdose_screened VARCHAR(10),
      overdose_results VARCHAR(255),
      overdose_treated VARCHAR(10),
      received_naloxone VARCHAR(10),
      overdose_referred VARCHAR(10),
      overdose_text VARCHAR(255),
      abscess_screened VARCHAR(10),
      abscess_results VARCHAR(255),
      abscess_treated VARCHAR(10),
      abscess_referred VARCHAR(10),
      abscess_text VARCHAR(255),
      alcohol_screened VARCHAR(10),
      alcohol_results VARCHAR(255),
      alcohol_treated VARCHAR(10),
      alcohol_referred VARCHAR(10),
      alcohol_text VARCHAR(255),
      cerv_cancer_screened VARCHAR(10),
      cerv_cancer_results VARCHAR(255),
      cerv_cancer_treated VARCHAR(10),
      cerv_cancer_referred VARCHAR(10),
      cerv_cancer_text VARCHAR(255),
      prep_screened VARCHAR(10),
      prep_results VARCHAR(255),
      prep_treated VARCHAR(10),
      prep_referred VARCHAR(10),
      prep_text VARCHAR(255),
      violence_screened VARCHAR(10),
      violence_results VARCHAR(255),
      violence_treated VARCHAR(10),
      violence_referred VARCHAR(10),
      violence_text VARCHAR(255),
      risk_red_counselling_screened VARCHAR(10),
      risk_red_counselling_eligibility VARCHAR(255),
      risk_red_counselling_support VARCHAR(10),
      risk_red_counselling_ebi_provided VARCHAR(10),
      risk_red_counselling_text VARCHAR(255),
      fp_screened VARCHAR(10),
      fp_eligibility VARCHAR(255),
      fp_treated VARCHAR(10),
      fp_referred VARCHAR(10),
      fp_text VARCHAR(255),
      mental_health_screened VARCHAR(10),
      mental_health_results VARCHAR(255),
      mental_health_support VARCHAR(10),
      mental_health_referred VARCHAR(10),
      mental_health_text VARCHAR(255),
      hiv_self_rep_status VARCHAR(50),
      last_hiv_test_setting VARCHAR(100),
      counselled_for_hiv VARCHAR(10),
      hiv_tested VARCHAR(10),
      test_frequency VARCHAR(100),
      received_results VARCHAR(10),
      test_results VARCHAR(100),
      linked_to_art VARCHAR(10),
      facility_linked_to VARCHAR(10),
      self_test_education VARCHAR(10),
      self_test_kits_given VARCHAR(100),
      self_use_kits VARCHAR (10),
      distribution_kits VARCHAR (10),
      self_tested VARCHAR(10),
      self_test_date DATE,
      self_test_frequency VARCHAR(100),
      self_test_results VARCHAR(100),
      test_confirmatory_results VARCHAR(100),
      confirmatory_facility VARCHAR(100),
      offsite_confirmatory_facility VARCHAR(100),
      self_test_linked_art VARCHAR(10),
      self_test_link_facility VARCHAR(255),
      hiv_care_facility VARCHAR(255),
      other_hiv_care_facility VARCHAR(255),
      initiated_art_this_month VARCHAR(10),
      active_art VARCHAR(10),
      eligible_vl VARCHAR(50),
      vl_test_done VARCHAR(100),
      vl_results VARCHAR(100),
      condom_use_education VARCHAR(10),
      post_abortal_care VARCHAR(10),
      linked_to_psychosocial VARCHAR(10),
      male_condoms_no VARCHAR(10),
      female_condoms_no VARCHAR(10),
      lubes_no VARCHAR(10),
      syringes_needles_no VARCHAR(10),
      pep VARCHAR(10),
      exposure_type VARCHAR(100),
      other_exposure_type VARCHAR(100),
      clinical_notes VARCHAR(255),
      appointment_date DATE,
      voided INT(11),
      constraint foreign key(client_id) references kp_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      index(client_id),
      index(client_id,visit_date)
    );
    SELECT "Successfully created etl_clinical_visit table";

    -- ------------ create table etl_peer_calendar-----------------------
    CREATE TABLE kp_etl.etl_peer_calendar (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      client_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      visit_id INT(11),
      encounter_provider INT(11),
      date_created DATE,
      hotspot_name VARCHAR(255),
      typology VARCHAR(255),
      avg_weekly_sex_acts INT(10),
      monthly_condom_requirements INT(10),
      avg_weekly_anal_sex_acts INT(10),
      monthly_lubes_requirements INT(10),
      avg_daily_injections INT(10),
      monthly_needle_requirements INT(10),
      years_in_sexwork_drugs INT(10),
      experienced_violence VARCHAR(10),
      service_provided_within_month VARCHAR(255),
      week1_n_and_s  INT(10),
      week1_male_condoms  INT(10),
      week1_lubes  INT(10),
      week1_female_condoms  INT(10),
      week1_health_edu  VARCHAR(100),
      week1_referred  VARCHAR(10),
      week1_self_test_kits_distributed INT(10),
      week1_received_clinical_service VARCHAR(10),
      week1_violence_reported VARCHAR(10),
      week1_remarks VARCHAR(255),
      week2_n_and_s  INT(10),
      week2_male_condoms  INT(10),
      week2_lubes  INT(10),
      week2_female_condoms  INT(10),
      week2_health_edu  VARCHAR(100),
      week2_referred  VARCHAR(10),
      week2_self_test_kits_distributed INT(10),
      week2_received_clinical_service VARCHAR(10),
      week2_violence_reported VARCHAR(10),
      week2_remarks VARCHAR(255),
      week3_n_and_s  INT(10),
      week3_male_condoms  INT(10),
      week3_lubes  INT(10),
      week3_female_condoms  INT(10),
      week3_health_edu  VARCHAR(100),
      week3_referred  VARCHAR(10),
      week3_self_test_kits_distributed INT(10),
      week3_received_clinical_service VARCHAR(10),
      week3_violence_reported VARCHAR(10),
      week3_remarks VARCHAR(255),
      week4_n_and_s  INT(10),
      week4_male_condoms  INT(10),
      week4_lubes  INT(10),
      week4_female_condoms  INT(10),
      week4_health_edu  VARCHAR(100),
      week4_referred  VARCHAR(10),
      week4_self_test_kits_distributed INT(10),
      week4_received_clinical_service VARCHAR(10),
      week4_violence_reported VARCHAR(10),
      week4_remarks VARCHAR(255),
      voided INT(11),
      CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(client_id, visit_date)
    );

    SELECT "Successfully created etl_peer_calendar table";

        -- ------------ create table etl_sti_Treatment-----------------------
    CREATE TABLE kp_etl.etl_sti_Treatment (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      client_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      visit_id INT(11),
      encounter_provider INT(11),
      date_created DATE,
      visit_reason VARCHAR(255),
      syndrome VARCHAR(255),
      other_syndrome VARCHAR(255),
      drug_prescription VARCHAR(255),
      other_drug_prescription VARCHAR(255),
      genital_exam_done VARCHAR(10),
      lab_referral VARCHAR(10),
      lab_form_number VARCHAR(100),
      referred_to_facility VARCHAR(10),
      facility_nmae VARCHAR(255),
      partner_referral_done VARCHAR(10),
      given_lubes VARCHAR(10),
      no_of_lubes INT(10),
      given_condoms VARCHAR(10),
      no_of_condoms INT(10),
      provider_comments VARCHAR(255),
      provider_name VARCHAR(255),
      appointment_date DATE,
      voided INT(11),
      CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(encounter_id),
      INDEX(client_id),
      INDEX(visit_reason),
      INDEX(given_lubes),
      INDEX(given_condoms)
    );

    SELECT "Successfully created etl_sti_Treatment table";
/*
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
      sti_screening_done VARCHAR(10),
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
      treated VARCHAR(10),
      referred VARCHAR(10),
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
      tb_results_status VARCHAR(100),
      start_anti_TB VARCHAR(10),
      tb_treatment_date date,
      tb_treatment VARCHAR(100),
      voided INT(11),
      CONSTRAINT FOREIGN KEY (client_id) REFERENCES kp_etl.etl_client_registration(client_id),
      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(encounter_id),
      INDEX(tb_results_status),
      INDEX(start_anti_TB),
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
      treated VARCHAR(10),
      referred VARCHAR(10),
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
      perpetrator VARCHAR(255),
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
    CREATE TABLE kp_etl.etl_counselling_services (
      uuid CHAR(38),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      client_id INT(11) NOT NULL ,
      location_id INT(11) DEFAULT NULL,
      visit_date DATE,
      visit_id INT(11),
      encounter_provider INT(11),
      date_created DATE,
      counselling_type VARCHAR(255),
      referred VARCHAR(10),
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
      referred VARCHAR(10),
      using_pep VARCHAR(10),
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
      CONSTRAINT unique_uuid UNIQUE(encounter_uuid),
      index(client_id),
      index(visit_date),
      index(tracing_type),
      index(tracing_status)
    );

    SELECT "Successfully created etl_hts_referral_and_linkage table";


    -- ------------- create etl_client_tracing table ------------------------
    CREATE TABLE kp_etl.etl_client_tracing (
      client_id INT(11) NOT NULL ,
      visit_id INT(11),
      visit_date DATE,
      location_id INT(11) DEFAULT NULL,
      encounter_uuid CHAR(38) NOT NULL,
      provider INT(11),
      encounter_id INT(11) NOT NULL PRIMARY KEY,
      date_created DATE,
      tracing_attempt_date DATETIME,
      tracing_type VARCHAR(100),
      tracing_outcome VARCHAR(100),
      negative_outcome_reason VARCHAR(100),
      negative_outcome_description VARCHAR(255),
      next_tracing_attempt_date DATE,
      final_tracing_status VARCHAR(100),
      voided INT(11),
      CONSTRAINT unique_uuid UNIQUE(encounter_uuid),
      INDEX(visit_date),
      INDEX(encounter_id),
      INDEX(client_id),
      INDEX(final_tracing_status),
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
      ever_tested VARCHAR(10),
      test_date DATE,
      test_results_status VARCHAR(100),
      current_in_care VARCHAR(10),
      referral VARCHAR(10),
      art_start_date DATE,
      treatment_facility VARCHAR(100),
      current_regimen VARCHAR(100),
      recent_vl_result VARCHAR(100),
      vl_test_date DATE,
      provider_referred_to VARCHAR(100),
      voided INT(11),

      CONSTRAINT unique_uuid UNIQUE(uuid),
      INDEX(visit_date),
      INDEX(encounter_id),
      INDEX(client_id),
      INDEX(ever_tested),
      INDEX(test_results_status),
      INDEX(treatment_facility)
    );
    SELECT "Successfully created etl_hiv_status table";

*/
    UPDATE kp_etl.etl_script_status SET stop_time=NOW() where id= script_id;


END$$


DROP PROCEDURE IF EXISTS create_datatools_tables$$
CREATE PROCEDURE create_datatools_tables()
BEGIN
DECLARE script_id INT(11);

-- Log start time
INSERT INTO kp_etl.etl_script_status(script_name, start_time) VALUES('KenyaEMR_Data_Tool', NOW());
SET script_id = LAST_INSERT_ID();

drop database if exists kp_datatools;
create database kp_datatools;

-- populate patient_client_enrollment table
create table kp_datatools.client_registration as
select
client_id ,
registration_date,
given_name,
middle_name,
family_name,
Gender,
DOB,
alias_name,
postal_address,
county,
sub_county,
location,
sub_location,
village,
phone_number,
alt_phone_number,
email_address,
national_id_number,
passport_number,
if(dead=1, "Yes", "NO") dead,
death_date,
voided

from kp_etl.etl_client_registration;

-- populate patient_client_enrollment table
create table kp_datatools.client_enrollment as
select
	  uuid,
      client_id,
      visit_id,
      visit_date,
      location_id,
      encounter_id,
      encounter_provider,
      date_created ,
      contacted_for_prevention,
      has_regular_free_sex_partner,
      year_started_sex_work,
      year_started_sex_with_men,
      year_started_drugs,
      has_expereienced_sexual_violence,
      has_expereienced_physical_violence,
      ever_tested_for_hiv,
      test_type,
      share_test_results,
      willing_to_test,
      test_decline_reason,
      receiving_hiv_care,
      care_facility_name,
      ccc_number,
      vl_test_done,
      vl_results_date,
      contact_for_appointment,
      contact_method,
      buddy_name,
      buddy_phone_number,
      voided



from kp_etl.etl_client_enrollment;

UPDATE kp_etl.etl_script_status SET stop_time=NOW() where id= script_id;

END$$


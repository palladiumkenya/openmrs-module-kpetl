
SET @OLD_SQL_MODE=@@SQL_MODE$$
SET SQL_MODE=''$$
DROP PROCEDURE IF EXISTS sp_populate_etl_client_registration$
CREATE PROCEDURE sp_populate_etl_client_registration$()
BEGIN
-- initial set up of etl_client_registration table
SELECT "Processing client registration data ", CONCAT("Time: ", NOW());
insert into kp_etl.etl_client_registration(
client_id,
registration_date,
given_name,
middle_name,
family_name,
Gender,
DOB,
dead,
voided,
death_date)
select
p.person_id,
p.date_created,
p.given_name,
p.middle_name,
p.family_name,
p.gender,
p.birth,
p.dead,
p.voided,
p.death_date
FROM (
select
p.person_id,
p.date_created,
pn.given_name,
pn.middle_name,
pn.family_name,
p.gender,
p.birth,
p.dead,
p.voided,
p.death_date
from person p
left join patient pa on pa.patient_id=p.person_id
left join person_name pn on pn.person_id = p.person_id and pn.voided=0
where p.voided=0
GROUP BY p.person_id
) p
ON DUPLICATE KEY UP given_name = p.given_name, middle_name=p.middle_name, family_name=p.family_name;

-- update etl_client_registration with patient attributes: birthplace, citizenship, mother_name, phone number and kin's details
update kp_etl.etl_client_registration r
left outer join
(
select
pa.person_id,
max(if(pat.uuid='aec1b592-1d8a-11e9-ab14-d663bd873d93', pa.value, null)) as alias_name,
max(if(pat.uuid='b2c38640-2603-4629-aebd-3b54f33f1e3a', pa.value, null)) as phone_number,
max(if(pat.uuid='94614350-84c8-41e0-ac29-86bc107069be', pa.value, null)) as alt_phone_number,
max(if(pat.uuid='b8d0b331-1d2d-4a9a-b741-1816f498bdb6', pa.value, null)) as email_address
from person_attribute pa
inner join
(
select
pat.person_attribute_type_id,
pat.name,
pat.uuid
from person_attribute_type pat
where pat.retired=0
) pat on pat.person_attribute_type_id = pa.person_attribute_type_id
and pat.uuid in (
	'aec1b592-1d8a-11e9-ab14-d663bd873d93', -- alias_name
	'b2c38640-2603-4629-aebd-3b54f33f1e3a', -- phone contact
	'94614350-84c8-41e0-ac29-86bc107069be', -- alternative phone contact
	'b8d0b331-1d2d-4a9a-b741-1816f498bdb6' -- email address

	)
where pa.voided=0
group by pa.person_id
) att on att.person_id = r.client_id
set r.alias_name = att.alias_name,
  r.phone_number=att.phone_number,
	r.alt_phone_number=att.alt_phone_number,
	r.email_address=att.email_address;


update kp_etl.etl_client_registration r
join (select pi.patient_id,
max(if(pit.uuid='49af6cdc-7968-4abb-bf46-de10d7f4859f',pi.identifier,null)) national_id,
max(if(pit.uuid='aec1b20e-1d8a-11e9-ab14-d663bd873d93',pi.identifier,null)) passport_number
from patient_identifier pi
join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
where voided=0
group by pi.patient_id) pid on pid.patient_id=d.patient_id
set
	r.national_id_number=pid.national_id,
	r.passport_number=pid.passport_number;

update kp_etl.etl_client_registration r
join (select pa.person_id as client_id,
pa.address1 as postal_address,
pa.county_district as county,
pa.state_province as sub_county,
pa.address4 as location,
pa.address5 as sub_location,
pa.city_village as village
from person_address pa
group by person_id) pstatus on pstatus.client_id=r.client_id
set r.postal_address=pstatus.postal_address,
r.county=pstatus.county,
r.sub_county= pstatus.sub_county,
r.location= pstatus.location,
r.sub_location= pstatus.sub_location,
r.village= pstatus.village;

END$$

DROP PROCEDURE IF EXISTS sp_populate_etl_client_social_status$$
CREATE PROCEDURE sp_populate_etl_client_social_status()
BEGIN
SELECT "Processing client social status data ", CONCAT("Time: ", NOW());
insert into kp_etl.etl_client_social_status (
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    key_population_type,
    hot_spot,
    weekly_sex_acts,
    weekly_anal_sex_acts,
    daily_drug_injections,
    avg_weekly_drug_injections,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       e.encounter_time as visit_date,
       e.location_id,
       e.encounter_id,
       e.creator,
       e.date_created,
       max(if(o.concept_id=164930,(case o.value_coded when 164982 then "Female sex worker" when 160578 then "Male who have sex with Men" when 164981 then "Male sex worker" when 160666
  then  "People who use drugs" when 157351 then "People who inject drugs"  else "" end),null)) as key_population_type,
       max(if(o.concept_id=164984,o.value_coded,null)) as hot_spot,
       max(if(o.concept_id=164986,o.value_numeric,null)) as weekly_sex_acts,
       max(if(o.concept_id=164987,o.value_numeric,null)) as weekly_anal_sex_acts,
       max(if(o.concept_id=164988,o.value_numeric,null)) as daily_drug_injections,
       max(if(o.concept_id=164989,o.value_numeric,null)) as avg_weekly_drug_injections,
       e.voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid='f02eea5e-1f42-11e9-ab14-d663bd873d93'
 ) et on et.encounter_type_id=e.encounter_type
       join openmrs.patient p on p.patient_id=e.patient_id and p.voided=0
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (164930,164984,164986,164987,164988,164989)
where e.voided=0
group by e.patient_id, e.encounter_id;
SELECT "Completed processing KP Social status data", CONCAT("Time: ", NOW());
END$$


DROP PROCEDURE IF EXISTS sp_populate_etl_client_enrollment$$
CREATE PROCEDURE sp_populate_etl_client_enrollment()
BEGIN
SELECT "Processing client enrollment data ", CONCAT("Time: ", NOW());
insert into kp_etl.etl_client_enrollment (
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    enrollment_service_area,
    contacted_for_prevention,
    contact_person_name,
    regular_free_sexual_partner ,
    free_sexual_partner_name,
    free_sexual_partner_alias,
    free_sexual_partner_contact,
    contact_regular_sexual_partner ,
    sex_work_startdate ,
    sex_with_men_startdate ,
    drug_startdate ,
    sexual_violence_experienced,
    sexual_violence_ordeal,
    physical_violence_experienced,
    physical_violence_ordeal,
    contact_clinical_appointment,
    contact_method,
    treatment_supporter_name,
    treatment_supporter_alias,
    treatment_supporter_contact,
    treatment_supporter_alt_contact,
    enrollment_notes,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       e.encounter_datetime as visit_date,
       e.location_id,
       e.encounter_id,
       e.creator,
       e.date_created,
       max(if(o.concept_id=160540,o.value_coded,null)) as enrollment_service_area,
       max(if(o.concept_id=164983,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as contacted_for_prevention,
       max(if(o.concept_id=1473,o.value_text,null)) as contact_person_name,
       max(if(o.concept_id=165006,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as regular_free_sexual_partner,
       max(if(o.concept_id=161135,o.value_text,null)) as free_sexual_partner_name,
       max(if(o.concept_id=165007,o.value_text,null)) as free_sexual_partner_alias,
       max(if(o.concept_id=159635,o.value_text,null)) as free_sexual_partner_contact,
       max(if(o.concept_id=165008,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as contact_regular_sexual_partner,
       max(if(o.concept_id=165009,o.value_datetime,null)) as sex_work_startdate,
       max(if(o.concept_id=165010,o.value_datetime,null)) as sex_with_men_startdate,
       max(if(o.concept_id=165011,o.value_datetime,null)) as drug_startdate,
       max(if(o.concept_id=123160,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as sexual_violence_experienced,
       max(if(o.concept_id=165012,o.value_text,null)) as sexual_violence_ordeal,
       max(if(o.concept_id=165013,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as physical_violence_experienced,
       max(if(o.concept_id=165014,o.value_text,null)) as physical_violence_ordeal,
       max(if(o.concept_id=165015,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as contact_clinical_appointment,
       max(if(o.concept_id=164966,(case o.value_coded when 161642 then "Treatment supporter" when 165016 then "Peer educator"  when 1555 then "Outreach worker"
   when 159635 then "Phone number" else "" end),null)) as contact_method,
       max(if(o.concept_id=160638,o.value_text,null)) as treatment_supporter_name,
       max(if(o.concept_id=165017,o.value_text,null)) as treatment_supporter_alias,
       max(if(o.concept_id=160642,o.value_text,null)) as treatment_supporter_contact,
       max(if(o.concept_id=159635,o.value_text,null)) as treatment_supporter_alt_contact,
       max(if(o.concept_id=161011,o.value_text,null)) as enrollment_notes,
       e.voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid='c7f47a56-207b-11e9-ab14-d663bd873d93'
 ) et on et.encounter_type_id=e.encounter_type
       join patient p on p.patient_id=e.patient_id and p.voided=0
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (160540,164983,1473,165006,161135,165007,159635,165008,165009,
       165010,165011,123160,165012,165013,165014,165015,164966,160638,165017,160642,159635,161011)
where e.voided=0
group by e.patient_id, e.encounter_id;
SELECT "Completed processing KP client enrollment data", CONCAT("Time: ", NOW());
END$$

update kp_etl.etl_client_enrollment e
join (select pi.patient_id,
     max(if(pit.uuid='b7bfefd0-239b-11e9-ab14-d663bd873d93',pi.identifier,null)) as unique_client_no
      from patient_identifier pi
     join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
      where voided=0
      group by pi.patient_id) pid on pid.patient_id=e.client_id
set
    e.unique_client_no=pid.unique_client_no;



-- ------------- populate etl_client_triage--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_client_triage$$
CREATE PROCEDURE sp_populate_etl_client_triage()
	BEGIN
		SELECT "Processing Triage ", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_triage(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    weight,
    height,
    systolic_pressure,
    diastolic_pressure,
    temperature,
    pulse_rate,
    respiratory_rate,
    oxygen_saturation,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
       max(if(o.concept_id=5090,o.value_numeric,null)) as height,
       max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_pressure,
       max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_pressure,
       max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
       max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
       max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
       max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('55e67467-bd0b-4940-82c2-3281938afde3')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (5089,5090,5085,5086,5088,5087,5242,5092)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing Triage data ", CONCAT("Time: ", NOW());
		END$$


-- ------------- populate etl_client_complaints--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_client_complaints$$
CREATE PROCEDURE sp_populate_etl_client_complaints()
  BEGIN
    SELECT "Processing Complaints ", CONCAT("Time: ", NOW());
    INSERT INTO kp_etl.etl_client_complaints(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
complaint_exists,
complaint_type,
nature_of_complaint,
onset_date,
duration,
remarks,
voided
)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=5219,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as complaint_exists,
   max(if(o.concept_id=124957,(case o.value_coded when 151 then "Abdominal pain"
when 141631 then "Abnormal Uterine Bleeding"
when 121543 then "Anxiety"
when 119537 then "Depression"
when 148035 then "Back pain"
when 840 then "Bloody Urine"
when 131021 then "Breast Pain"
when 120749 then "Chest Pain"
when 871 then "Cold and Chills"
when 120345 then "Confusion"
when 119574 then "Delirium"
when 113054 then "Convulsions"
when 206 then "Seizure"
when 143264 then "Cough"
when 143129 then "Crying Infant"
when 142412 then "Diarrhea"
when 122496 then "Difficult in breathing"
when 118789 then "Difficulty in swallowing"
when 141830 then "Dizziness"
when 141585 then "Ear Pain"
when 141128 then "Epigastric Pain"
when 131040 then "Eye pain"
when 114399 then "Facial Pain"
when 162626 then "Fatigue/weakness"
when 140238 then "Fever"
when 140070 then "Flank Pain"
when 123396 then "Vaginal Discharge"
when 142247 then "Discharge from Penis"
when 135462 then "Genital Ulcer"
when 139084 then "Headache"
when 117698 then "Hearing Loss"
when 116214 then "Hypotension"
when 112989 then "Shock"
when 879 then "Itchiness/Pruritus"
when 116558 then "Joint Pain"
when 114395 then "Leg Pain"
when 135595 then "Loss of Appetite"
when 135488 then "Lymphadenopathy"
when 121657 then "Memory Loss"
when 144576 then "Coma"
when 116334 then "Lethargy"
when 131015 then "Mouth Pain"
when 111721 then "Mouth Ulceration"
when 133028 then "Muscle cramps"
when 133632 then "Muscle Pain"
when 5978 then "Nausea"
when 133469 then "Neck Pain"
when 133027 then "Night sweats"
when 132653 then "Numbness"
when 125225 then "Pain when Swallowing"
when 131034 then "Pelvic Pain"
when 5953  then "Poor Vision"
when 512 then "Rash"
when 127777 then "Red Eye"
when 113224 then "Running/Blocked nose"
when 131032 then "Scrotal Pain"
when 126535 then "Shoulder Pain"
when 141597 then "Sleep Disturbance"
when 158843 then "Sore Throat"
when 140941 then "Excessive Sweating"
when 125198 then "Swollen Legs"
when 112200 then "Tremors"
when 160208 then "Urinary Symptoms"
when 111525 then "Vertigo"
when 122983 then "Vomiting"
when 832 then "Weight Loss"
when 5622 then "Other" else "" end),null)) as complaint_type,
   max(if(o.concept_id=163042,o.value_text,null)) as nature_of_complaint,
   max(if(o.concept_id=159948,o.value_datetime,null)) as onset_date,
   max(if(o.concept_id=159368,o.value_numeric,null)) as duration,
   max(if(o.concept_id=160632,o.value_text,null)) as remarks,

   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('2c3cf276-3676-11e9-b210-d663bd873d93')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (5219,124957,163042,159948,159368,160632)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed processing complaints data ", CONCAT("Time: ", NOW());
    END$$


    -- ------------- populate etl_chronic_illness--------------------------------

    DROP PROCEDURE IF EXISTS sp_populate_etl_chronic_illness$$
    CREATE PROCEDURE sp_populate_etl_chronic_illness()
      BEGIN
SELECT "Processing chronic illness ", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_chronic_illness(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    illness_exists,
    illness_type,
    nature_of_illness,
    onset_date,
    duration,
    remarks,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=162747,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as illness_exists,
       max(if(o.concept_id=1284,(case o.value_coded when 149019 then "Alzheimer's Disease and other Dementias"
    when 148432 then "Arthritis"
    when 153754 then "Asthma"
    when 159351 then "Cancer"
    when 119270 then "Cardiovascular diseases"
    when 120637 then "Chronic Hepatitis"
    when 145438 then "Chronic Kidney Disease"
    when 1295 then "Chronic Obstructive Pulmonary Disease(COPD)"
    when 120576 then "Chronic Renal Failure"
    when 119692 then "Cystic Fibrosis"
    when 120291 then "Deafness and Hearing impairment"
    when 119481 then "Diabetes"
    when 118631 then "Endometriosis"
    when 117855 then "Epilepsy"
    when 117789 then "Glaucoma"
    when 139071 then "Heart Disease"
    when 115728 then "Hyperlipidaemia"
    when 117399 then "Hypertension"
    when 117321 then "Hypothyroidism"
    when 151342 then "Mental illness"
    when 133687 then "Multiple Sclerosis"
    when 115115 then "Obesity"
    when 114662 then "Osteoporosis"
    when 117703 then "Sickle Cell Anaemia"
    when 118976 then "Thyroid disease"

    else "" end),null)) as illness_type,
       max(if(o.concept_id=163042,o.value_text,null)) as nature_of_illness,
       max(if(o.concept_id=159948,o.value_datetime,null)) as onset_date,
       max(if(o.concept_id=159368,o.value_numeric,null)) as duration,
       max(if(o.concept_id=160632,o.value_text,null)) as remarks,

       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('26bb869b-b569-4acd-b455-02c853e9f1e6')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (162747,1284,163042,159948,159368,160632)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing chronic illness data ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate sp_populate_etl_allergies--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_allergies$$
CREATE PROCEDURE sp_populate_etl_allergies()
  BEGIN
    SELECT "Processing allergies ", CONCAT("Time: ", NOW());
    INSERT INTO kp_etl.etl_allergies(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
allergy_exists,
causative_agent,
reaction,
severity,
onset_date,
voided
)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=5219,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as illness_exists,
   max(if(o.concept_id=160643,(case o.value_coded when 162543 then "Beef"
  when 72609 then "Caffeine"
  when 162544 then "Chocolate"
  when 162545 then "Dairy Food"
  when 162171 then "Eggs"
  when 162546 then "Fish"
  when 162547 then "Milk Protein"
  when 162172 then "Peanuts"
  when 162175 then "Shellfish"
  when 162176 then "Soy"
  when 162548 then "Strawberries"
  when 162177 then "Wheat"
  when 162542 then "Adhesive Tape"
  when 162536 then "Bee Stings"
  when 162537 then "Dust"
  when 162538 then "Latex"
  when 162539 then "Mold"
  when 162540 then "Pollen"
  when 162541 then "Ragweed"
  when 5622 then "Other"
  else "" end),null)) as causative_agent,
   max(if(o.concept_id=159935,(case o.value_coded when 1067 then "Unknown"
  when 121629 then "Anaemia"
  when 148888 then "Anaphylaxis"
  when 148787 then "Angioedema"
  when 120148 then "Arrhythmia"
  when 108 then "Bronchospasm"
  when 143264 then "Cough"
  when 142412 then "Diarrhea"
  when 118773 then "Dystonia"
  when 140238 then "Fever"
  when 140039 then "Flushing"
  when 139581 then "GI upset"
  when 139084 then "Headache"
  when 159098 then "Hepatotoxicity"
  when 111061 then "Hives"
  when 117399 then "Hypertension"
  when 879 then "Itching"
  when 121677 then "Mental status change"
  when 159347 then "Musculoskeletal pain"
  when 121 then "Myalgia" else "" end) ,null)) as reaction,
   max(if(o.concept_id=162760,(case o.value_coded when 1498 then "Mild"
  when 1499 then "Moderate"
  when 1500 then "Severe"
  when 162819 then "Fatal"
  when 1067 then "Unknown"
  else "" end) ,null)) as severity,
   max(if(o.concept_id=164428,o.value_datetime,null)) as onset_date,
   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('119362fb-6af6-4462-9fb2-7a09c43c9874')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (5219,160643,159935,162760,164428)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed processing allergies data ", CONCAT("Time: ", NOW());
    END$$

    -- ------------- populate sp_populate_etl_allergies--------------------------------

    DROP PROCEDURE IF EXISTS etl_pregnancy_fp_cacx_screening$$
    CREATE PROCEDURE sp_populate_etl_pregnancy_fp_cacx_screening()
      BEGIN
SELECT "Processing Pregnancy,FP and CaCx screening ", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_pregnancy_fp_cacx_screening(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    lmp,
    pregnant,
    edd,
    fp_status,
    elible_for_fp,
    fp_method,
    referred_for_fp,
    cacx_screening,
    cacx_screening_results,
    treated,
    referred,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=1427,o.value_datetime,null)) as lmp,
       max(if(o.concept_id=5272,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as pregnant,
       max(if(o.concept_id=5596,o.value_datetime,null)) as edd,
       max(if(o.concept_id=160653,(case o.value_coded when 965 then "On Family Planning"
      when 160652 then "Not using Family Planning"
      when 1360 then "Wants Family Planning"
      else "" end),null)) as fp_status,
       max(if(o.concept_id=165081,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as eligible_for_fp,
       max(if(o.concept_id=374,(case o.value_coded  when 160570 then "Emergency contraceptive pills"
    when 780 then "Oral Contraceptives Pills"
    when 5279 then "Injectible"
    when 1359 then "Implant"
    when 5275 then "Intrauterine Device"
    when 136163 then "Lactational Amenorhea Method"
    when 5278 then "Diaphram/Cervical Cap"
    when 5277 then "Fertility Awareness"
    when 1472 then "Tubal Ligation"
    when 190 then "Condoms"
    when 1489 then "Vasectomy(Partner)"
    when 162332 then "Undecided"
    else "" end) ,null)) as fp_method,
       max(if(o.concept_id=165082,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as referred_for_fp,
       max(if(o.concept_id=164934,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as cacx_screening,
       max(if(o.concept_id=164934,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as cacx_screening_results,
       max(if(o.concept_id=165052,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as treated,
       max(if(o.concept_id=1272,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as referred,
       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('55d0b03e-8977-4d3e-8941-3333712b1afe')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (1427,5272,5596,160653,165081,374,165082,164934,165052,1272)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing pregnancy, family planning and CaCz screening data ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate sp_populate_etl_adverse_drug_reaction--------------------------------

DROP PROCEDURE IF EXISTS etl_adverse_drug_reaction$$
CREATE PROCEDURE sp_populate_etl_adverse_drug_reaction()
  BEGIN
    SELECT "Processing adverse drug reaction", CONCAT("Time: ", NOW());
    INSERT INTO kp_etl.etl_adverse_drug_reaction(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
adverse_drug_reaction_exists,
causative_drug,
reaction,
severity,
onset_date,
action_taken,
voided
)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=162867,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as adverse_drug_reaction_exists,
   max(if(o.concept_id=1193,(case o.value_coded when 70056 then "Abicavir"
when 162298 then "ACE inhibitors"
when 70878 then "Allopurinol"
when 155060 then "Aminoglycosides"
when 162299 then "ARBs (angiotensin II receptor blockers)"
when 103727 then "Aspirin"
when 71647 then "Atazanavir"
when 72822 then "Carbamazepine"
when 162301 then "Cephalosporins"
when 73300 then "Chloroquine"
when 73667 then "Codeine"
when 74807 then "Didanosine"
when 75523 then "Efavirenz"
when 162302 then "Erythromycins"
when 75948 then "Ethambutol"
when 77164 then "Griseofulvin"
when 162305 then "Heparins"
when 77675 then "Hydralazine"
when 78280 then "Isoniazid"
when 794 then "Lopinavir/ritonavir"
when 80106 then "Morphine"
when 80586 then "Nevirapine"
when 80696 then "Nitrofurans"
when 162306 then "Non-steroidal anti-inflammatory drugs"
when 81723 then "Penicillamine"
when 81724 then "Penicillin"
when 81959 then "Phenolphthaleins"
when 82023 then "Phenytoin"
when 82559 then "Procainamide"
when 82900 then "Pyrazinamide"
when 83018 then "Quinidine"
when 767 then "Rifampin"
when 162307 then "Statins"
when 84309 then "Stavudine"
when 162170 then "Sulfonamides"
when 84795 then "Tenofovir"
when 84893 then "Tetracycline"
when 86663 then "Zidovudine"
when 5622 then "Other"
else "" end),null)) as causative_drug,
   max(if(o.concept_id=159935,(case o.value_coded when 1067 then "Unknown"
  when 121629 then "Anaemia"
  when 148888 then "Anaphylaxis"
  when 148787 then "Angioedema"
  when 120148 then "Arrhythmia"
  when 108 then "Bronchospasm"
  when 143264 then "Cough"
  when 142412 then "Diarrhea"
  when 118773 then "Dystonia"
  when 140238 then "Fever"
  when 140039 then "Flushing"
  when 139581 then "GI upset"
  when 139084 then "Headache"
  when 159098 then "Hepatotoxicity"
  when 111061 then "Hives"
  when 117399 then "Hypertension"
  when 879 then "Itching"
  when 121677 then "Mental status change"
  when 159347 then "Musculoskeletal pain"
  when 121 then "Myalgia"
  when 512 then "Rash"
  when 5622 then "Other"
  else "" end),null)) as reaction,
   max(if(o.concept_id=162760,(case o.value_coded when 1498 then "Mild"
  when 1499 then "Moderate"
  when 1500 then "Severe"
  when 162819 then "Fatal"
  when 1067 then "Unknown"
  else "" end),null)) as severity,
   max(if(o.concept_id=160753,o.value_datetime,null)) as onset_date,
   max(if(o.concept_id=1255,(case o.value_coded when 1257 then "CONTINUE REGIMEN"
when 1259 then "SWITCHED REGIMEN"
when 981 then "CHANGED DOSE"
when 1258 then "SUBSTITUTED DRUG"
when 1107 then "NONE"
when 1260 then "STOP"
when 5622 then "OTHER"
else "" end),null)) as action_taken,

   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('d7cfa460-2944-11e9-b210-d663bd873d93')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (162867,1193,159935,162760,160753,1255)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed processing adverse drug reaction data ", CONCAT("Time: ", NOW());
    END$$


    -- ------------- populate sp_populate_etl_immunization_screening--------------------------------

    DROP PROCEDURE IF EXISTS sp_populate_etl_immunization_screening$$
    CREATE PROCEDURE sp_populate_etl_immunization_screening()
      BEGIN
SELECT "Processing immunization screening", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_immunization_screening(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    immunization_done,
    immunization_type,
    immunization_date,
    immunization_side_effects,
    nature_of_side_effects,
    vaccine_validity,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=5585,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as immunization_done,
       max(if(o.concept_id=984,(case o.value_coded when 5665 then "ARV Prophylaxis, Cervical Cancer Vaccine"
   when 159711 then "Chickenpox (Varicella)"
   when 73193 then "Cholera Vaccine"
   when 73354 then "Diphtheria"
   when 129850 then "Flu (Influenza)"
   when 78032 then "Hepatitis A"
   when 77424 then "Hepatitis B"
   when 77429 then "Hib"
   when 159696 then "Human Papillomavirus (HPV)"
   when 159708 then "Measles"
   when 79409 then "Meningococcal"
   when 79554 then "Mumps"
   when 80193 then "Pneumococcal"
   when 82215 then "Polio"
   when 82243 then "Rotavirus"
   when 83533 then "Rubella"
   when 83563 then "Tetanus"
   when 129638 then "Whooping Cough (Pertussis)"
   when 1656 then "Rabies"
   when 83050 then "Smallpox"
   when 129658 then "Typhoid Fever"
   when 86022 then "Yellow Fever"

   else "" end),null)) as immunization_type,
       max(if(o.concept_id=1410,o.value_datetime,null)) as immunization_date,
       max(if(o.concept_id=160325,(case o.value_coded when 1065 then "Yes" when 1066 then "No"
     else "" end),null)) as immunization_side_effects,

       max(if(o.concept_id=163162,o.value_text,null)) as nature_of_side_effects,
       max(if(o.concept_id=160753,o.value_numeric,null)) as vaccine_validity,
       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('9b8c17cc-3420-11e9-b210-d663bd873d93')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (5585,984,1410,160325,163162,160753)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing Immunization screening data ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate sp_populate_etl_sti_screening--------------------------------

DROP PROCEDURE IF EXISTS etl_sti_screening$$
CREATE PROCEDURE sp_populate_etl_adverse_drug_reaction()
  BEGIN
    SELECT "Processing STI screening", CONCAT("Time: ", NOW());
    INSERT INTO kp_etl.etl_sti_screening(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
sti_screening_done,
reason,
provider_name,
voided
)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=161558,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as sti_screening_done,
   max(if(o.concept_id=164082,(case o.value_coded when 1068 then "Symptomatic"
  when 5006 then "Asymptomatic"
  when 163139 then "Quartely Screening"
  when 160523 then "Follow up"
  else "" end),null)) as reason,
   max(if(o.concept_id=1473,o.value_text,null)) as provider_name,

   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('83610d13-d4fc-42c3-8c1d-a403cd6dd073')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (161558,164082,1473)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed processing Immunization screening data ", CONCAT("Time: ", NOW());
    END$$

    -- ------------- populate sp_populate_etl_hepatitis_screening--------------------------------

    DROP PROCEDURE IF EXISTS etl_hepatitis_screening$$
    CREATE PROCEDURE sp_populate_etl_hepatitis_screening()
      BEGIN
SELECT "Processing hepatitis screening", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_hepatitis_screening(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    hepatitis_screening_done,
    results,
    treated,
    referred,
    remarks,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=164082,(case o.value_coded when 165019 then "Hepatitis B" when 165020 THEN "Hepatitis C" else "" end),null)) as hepatitis_screening_done,
       max(if(o.concept_id=1322,(case o.value_coded when 664 then "Negative"
      when 703 then "Positive"
      when 782 then "Vaccinated"
      else "" end),null)) as results,
       max(if(o.concept_id=165038,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as treated,
       max(if(o.concept_id=1272,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as referred,
       max(if(o.concept_id=160632,o.value_text,null)) as remarks,

       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('5c05a229-51b4-4b73-be13-0d93765a2a96')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (164082,1322,165038,1272,160632)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing Hepatitis screening data ", CONCAT("Time: ", NOW());
END$$



-- ------------- populate sp_populate_etl_tb_screening--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_tb_screening$$
CREATE PROCEDURE sp_populate_etl_tb_screening()
  BEGIN
    SELECT "Processing TB screening", CONCAT("Time: ", NOW());
    INSERT INTO kp_etl.etl_tb_screening(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
symptoms_present,
test_ordered,
sputum_smear_action,
chest_xray_action,
gene_xpert_action,
clinical_diagnosis,
invite_contacts,
ipt_evaluated,
tb_results_status,
start_anti_TB,
tb_treatment_date,
tb_treatment,
voided
)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=1729,(case o.value_coded when 159799 then "Cough of any duration"
when 1494 then "fever"
when 832 then "noticeable_weight_loss_poor_gain"
when 133027 then "night_sweats"
when 1066 THEN "None"
else "" end),null)) as symptoms_present,
   max(if(o.concept_id=1271,(case o.value_coded when 307 then "Sputum Smear" when 12 THEN "Chest Xray" when 162202 THEN "GeneXpert"else "" end),null)) as test_ordered,
   max(if(o.concept_id=307,(case o.value_coded when 703 then "Positive" when 664 THEN "Negative" else "" end),null)) as sputum_smear_action,
   max(if(o.concept_id=12,(case o.value_coded when 1115 then "Normal" when 152526 THEN "Abnormal" else "" end),null)) as chest_xray_action,
   max(if(o.concept_id=162202,(case o.value_coded when 664 then "Negative" when 162203 THEN "Resistant TB Detected" when 162204 THEN "Non-Resistant TB Detected" when 164104 THEN "Indeterminate-Resistant TB Detected"
  when 163611 then "Invalid"
  when 1138 then "Indeterminate"
  else "" end),null)) as gene_xpert_action,
   max(if(o.concept_id=163752,(case o.value_coded when 703 then "Positive" when 664 THEN "Negative" else "" end),null)) as clinical_diagnosis,
   max(if(o.concept_id=163414,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as invite_contacts,
   max(if(o.concept_id=162275,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as ipt_evaluated,
   max(if(o.concept_id=1659,(case o.value_coded when 1660 then "No TB Signs" when 142177 THEN "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "TB Screening Not Done" else "" end),null)) as ipt_evaluated,
   max(if(o.concept_id=162309,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as start_anti_TB,
   max(if(o.concept_id=1113,o.value_datetime,null)) as tb_treatment_date,
   max(if(o.concept_id=1111,o.value_coded,null)) as tb_treatment,
   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('32e5ac6f-80cf-4908-aa88-200e3e199c68')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (1729,1271,307,12,162202,163752,163414,162275,
   1659,162309,1113,1111)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed processing TB screening data ", CONCAT("Time: ", NOW());
    END$$

    -- ------------- populate sp_populate_etl_systems_review--------------------------------

    DROP PROCEDURE IF EXISTS sp_populate_etl_systems_review$$
    CREATE PROCEDURE sp_populate_etl_systems_review()
      BEGIN
SELECT "Processing review of systems", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_systems_review(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    body_systems,
    findings,
    finding_notes,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=164388,(case o.value_coded
     when 1120 then "Skin"
     when 163309 then "Eyes"
     when 164936 then "ENT"
     when 1123 then "Chest"
     when 1124 then "CVS"
     when 1125 then "Abdomen"
     when 164937 then "CNS"
     when 1126 then "Genitourinary"
     else "" end),null)) as body_systems,
       max(if(o.concept_id=1069,(case o.value_coded when 150555 then "Abscess"
    when 125201 then "Swelling/Growth"
    when 135591 then "Hair Loss"
    when 136455 then "Itching"
    when 507 then "Kaposi Sarcoma"
    when 1249 then "Skin eruptions/Rashes"
    when 5244 then "Oral sores"
    when 123074 then "Visual Disturbance"
    when 140940 then "Excessive tearing"
    when 131040 then "Eye pain"
    when 127777 then "Eye redness"
    when 140827 then "Light sensitive"
    when 139100 then "Itchy eyes"
    when 148517 then "Apnea"
    when 139075 then "Hearing disorder"
    when 119558 then "Dental caries"
    when 118536 then "Erythema"
    when 106 then "Frequent colds"
    when 147230 then "Gingival bleeding"
    when 135841 then "Hairy cell leukoplakia"
    when 117698 then "Hearing loss"
    when 138554 then "Hoarseness"
    when 507 then "Kaposi Sarcoma"
    when 152228 then "Masses"
    when 128055 then "Nasal discharge"
    when 133499 then "Nosebleed"
    when 160285 then "Pain"
    when 110099 then "Post nasal discharge"
    when 126423 then "Sinus problems"
    when 126318 then "Snoring"
    when 158843 then "Sore thoat"
    when 5244 then "Oral sores"
    when 5334 then "Thrush"
    when 123588 then "Tinnitus"
    when 124601 then "Toothache"
    when 123919 then "Ulcers"
    when 111525 then "Vertigo"
    when 146893 then "Bronchial breathing"
    when 127640 then "Crackles"
    when 145712 then "Dullness"
    when 164440 then "Reduced breathing"
    when 127639 then "Respiratory distress"
    when 5209 then "Wheezing"
    when 140147 then "Elevated blood pressure"
    when 136522 then "Irregular heartbeat"
    when 562 then "Cardiac murmur"
    when 130560 then "Cardiac rub"
    when 150915 then "Abdominal distension"
    when 5008 then "Hepatomegaly"
    when 5103 then "Abdominal mass"
    when 5009 then "Splenomegaly"
    when 5105 then "Abdominal tenderness"
    when 118872 then "Altered sensations"
    when 1836 then "Bulging fontenelle"
    when 150817 then "Abnormal reflexes"
    when 120345 then "Confusion"
    when 157498 then "Limb weakness"
    when 112721 then "Stiff neck"
    when 136282 then "Kernicterus"
    when 147241 then "Bleeding"
    when 154311 then "Rectal discharge"
    when 123529 then "Urethral discharge"
    when 123396 then "Vaginal discharge"
    when 124087 then "Ulceration"
    else "" end),null)) as findings,
       max(if(o.concept_id=160632,o.value_text,null)) as findings_notes,
       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('5568ab72-e951-4683-875e-c5781b6f7b81')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (164388,1069,160632)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing review of systems data ", CONCAT("Time: ", NOW());
END$$


    -- ------------- populate sp_populate_etl_diagnosis_treatment-------------------------------

    DROP PROCEDURE IF EXISTS sp_populate_etl_diagnosis_treatment$$
    CREATE PROCEDURE sp_populate_etl_diagnosis_treatment()
      BEGIN
SELECT "Processing Diagnosis and treatment plan", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_diagnosis_treatment(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    diagnosis,
    treatment_plan,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=6042,o.value_coded,null)),
       max(if(o.concept_id=163104,o.value_text,null)),
       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('928ea6b2-3425-4ee9-854d-daa5ceaade03')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (6042,163104)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing diagnosis and treatment data ", CONCAT("Time: ", NOW());
END$$

-- ------------- populate sp_populate_etl_clinical_notes-------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_clinical_notes$$
CREATE PROCEDURE sp_populate_etl_clinical_notes()
  BEGIN
    SELECT "Processing clinical notes", CONCAT("Time: ", NOW());
    INSERT INTO kp_etl.etl_clinical_notes(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
clinical_notes,
voided
)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=160632,o.value_text,null)),
   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('bcbf6e3f-a2fc-421b-90a3-473a3158c796')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (160632)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed processing clinical notes data ", CONCAT("Time: ", NOW());
    END$$




    -- ------------- populate sp_populate_etl_appointment-------------------------------

    DROP PROCEDURE IF EXISTS sp_populate_etl_appointment$$
    CREATE PROCEDURE sp_populate_etl_appointment()
      BEGIN
SELECT "Processing appointments", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_appointment(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    appointment_date,
    appointment_type,
    appointment_notes,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=5096,o.value_datetime,null)),
       max(if(o.concept_id=160288,(case o.value_coded
     when 160523 then "Follow up"
     when 1283 then "Lab tests"
     when 159382 then "Counseling"
     when 160521 then "Pharmacy Refill"
     when 5622 then "Other"
     else "" end),null)) as appointment_type,
       max(if(o.concept_id=163042,o.value_text,null)),
       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('66609dee-3438-11e9-b210-d663bd873d93')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (5096,160288,163042)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing appointments data ", CONCAT("Time: ", NOW());
END$$

-- ------------- populate sp_populate_etl_alcohol_drugs_risk_screening-------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_alcohol_drugs_risk_screening$$
CREATE PROCEDURE sp_populate_etl_alcohol_drugs_risk_screening()
  BEGIN
    SELECT "Processing alcohol, drugs and risk screening", CONCAT("Time: ", NOW());
    INSERT INTO kp_etl.etl_alcohol_drugs_risk_screening(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
screened_for,
results,
treated,
referred,
remarks,
voided
)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=164082,(case o.value_coded when 165023 then "Alcohol" when 165025 then "Risk" when 165025 then "Drugs" else "" end),null)) as screened_for,
   max(if(o.concept_id=165028,(case o.value_coded
 when 664 then "Negative"
 when 703 then "Positive"
 else "" end),null)) as results,
   max(if(o.concept_id=165038,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as treated,
   max(if(o.concept_id=1272,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as referred,
   max(if(o.concept_id=160632,o.value_text,null)),
   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('981c1420-4e83-4656-beb1-2461c45de532')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (164082,165028,165038,1272,160632)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed processing alcohol, drugs and risk assessment data ", CONCAT("Time: ", NOW());
    END$$

    -- ------------- populate sp_populate_etl_violence_screening-------------------------------
    DROP PROCEDURE IF EXISTS sp_populate_etl_violence_screening$$
    CREATE PROCEDURE sp_populate_etl_violence_screening()
      BEGIN
SELECT "Processing violence screening", CONCAT("Time: ", NOW());

INSERT INTO kp_etl.etl_violence_screening(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
form_of_violence,
place_of_violence,
incident_date,
target,
perpetrator,
intervention_date,
referral_ordered,
place_of_referral,
referral_date,
outcome_status,
action_plan,
resolution_date,
program_officer_name,
voided
)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=141814,(case o.value_coded
 when 123007 then "Verbal abuse"
 when 152292 then "Physical abuse"
 when 126312 then "Discrimination"
 when 152370 then "Sexual abuse/Rape"
 when 156761 then "illegal arrest"
 when 5622 then "other"
 else "" end),null)) as form_of_violence,
   max(if(o.concept_id=162721,o.value_text,null)),
   max(if(o.concept_id=160753,o.value_datetime,null)),
   max(if(o.concept_id=165013,(case o.value_coded
 when 978 then "Self"
 when 163488 then "Group"
 else "" end),null)) as target,
   max(if(o.concept_id=160658,(case o.value_coded
 when 123163 then "Police"
 when 5620 then "Family"
 when 110360 then "Religeous group"
 when 1540 then "Clients"
 when 163096 then "Health care provider"
 when 159928 then "Education institution"
 when 133651 then "Local gangs"
 else "" end),null)) as perpetrator,
   max(if(o.concept_id=162869,o.value_datetime,null)),
   max(if(o.concept_id=1272,(case o.value_coded
       when 1370 then "HIV Testing service"
       when 160570 then "Emergency contraception"
       when 162978 then "Reported to police"
       when 5490 then "Psychosocial counselling"
       when 1691 then "Pep provided"
       when 163559 then "STI Screening and treatment"
       when 162717 then "Legal support"
       when 432 then "Medical examination"
       when 123157 then "Post rape care form filled"
       when 5622 then "other"
       else "" end),null)) as referral_ordered,
   max(if(o.concept_id=161550,o.value_text,null)),
   max(if(o.concept_id=163181,o.value_datetime,null)),
   max(if(o.concept_id=160433,(case o.value_coded
 when 159 then "Dead"
 when 162277 then "In prison"
 when 1536 then "At home"
 when 163321 then "In safe space"
 when 5485 then "Hospitalized"
 when 5622 then "other"
 else "" end),null)) as outcome_status,
   max(if(o.concept_id=164378,o.value_text,null)),
   max(if(o.concept_id=161561,o.value_datetime,null)),
   max(if(o.concept_id=164141,o.value_text,null)),

   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('7b69daf5-b567-4384-9d29-f020c408d613')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (141814,162721,160753,165013,160658,162869,1272,161550,163181,160433,164378,161561,164141)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed violence screening data ", CONCAT("Time: ", NOW());
    END$$



-- ------------- populate sp_populate_counselling_services-------------------------------

DROP PROCEDURE IF EXISTS sp_populate_counselling_services$$
CREATE PROCEDURE sp_populate_counselling_services()
  BEGIN
    SELECT "Processing counselling services", CONCAT("Time: ", NOW());
    INSERT INTO kp_etl.counselling_services(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
counselling_type,
referred,
remarks,
voided
)select * from openmrs.encounter;
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   (e.encounter_datetime) as visit_date,
   e.location_id,
   e.encounter_id as encounter_id,
   e.creator,
   e.date_created as date_created,
   max(if(o.concept_id=165056,(case o.value_coded
 when 5490 then "Psychosocial counselling"
 when 1370 then "HIV counselling"
 when 161594 then "Condom use counselling"
 when 155791 then "Counselling for alcoholism"
 when 1382 then "Family planning"
 when 1455 then "Tobacco use counselling"
 when 164882 then "STD Prevention counselling"
 when 5622 then "Other"
 else "" end),null)) as counselling_type,
   max(if(o.concept_id=1272,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)),
   max(if(o.concept_id=160632,o.value_text,null)),
   e.voided as voided
    from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid in('28883f27-dfd1-4ce5-89f0-2a4f87974d15')
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
      and o.concept_id in (165056,1272,160632)
    where e.voided=0
    group by e.patient_id, e.encounter_id, visit_date;
    SELECT "Completed processing counselling data ", CONCAT("Time: ", NOW());
    END$$


    -- ------------- populate sp_populate_etl_prep_pep_screening-------------------------------
    DROP PROCEDURE IF EXISTS sp_populate_etl_prep_pep_screening$$
    CREATE PROCEDURE sp_populate_etl_prep_pep_screening()
      BEGIN
SELECT "Processing PrEp/PEp screening", CONCAT("Time: ", NOW());

INSERT INTO kp_etl.etl_prep_pep_screening(
    uuid,
    client_id,
    visit_id,
    visit_date,
    location_id,
    encounter_id,
    encounter_provider,
    date_created,
    screened_for,
    status,
    referred,
    using_pep,
    exposure_type,
    remarks,
    voided
    )
select
       e.uuid,
       e.patient_id,
       e.visit_id,
       (e.encounter_datetime) as visit_date,
       e.location_id,
       e.encounter_id as encounter_id,
       e.creator,
       e.date_created as date_created,
       max(if(o.concept_id=164082,(case o.value_coded
     when 164845 then "PEP Use"
     when 165062 then "PrEP"
     else "" end),null)) as screened_for,
       max(if(o.concept_id=165028,(case o.value_coded
     when 664 then "Negative"
     when 703 then "Positive"
     else "" end),null)) as status,
       max(if(o.concept_id=1272,(case o.value_coded
     when 1065 then "Yes"
     when 1066 then "No"
     else "" end),null)) as referred,
       max(if(o.concept_id=164845,(case o.value_coded
   when 1065 then "Yes"
   when 1066 then "No"
   else "" end),null)) as using_pep,
       max(if(o.concept_id=165046,(case o.value_coded
     when 127910 then "Rape"
     when 165045 then "Condom burst"
     when 160632 then "Others"
     else "" end),null)) as exposure_type,
       max(if(o.concept_id=160632,o.value_text,null)),

       e.voided as voided
from encounter e
       inner join
 (
 select encounter_type_id, uuid, name from encounter_type where uuid in('b06625d4-dfe4-458c-93fa-e878c8370733')
 ) et on et.encounter_type_id=e.encounter_type
       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (164082,165028,1272,164845,160632 )
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed PrEp/PEp screening data ", CONCAT("Time: ", NOW());

END$$


                      -- ------------------------------------ populate etl_hts_test table ----------------------------------------

                      DROP PROCEDURE IF EXISTS sp_populate_hts_test$$
                      CREATE PROCEDURE sp_populate_hts_test()
                        BEGIN
                          SELECT "Processing hts tests";
                          INSERT INTO kp_etl.etl_hts_test (
                              client_id,
                              visit_id,
                              encounter_id,
                              encounter_uuid,
                              encounter_location,
                              creator,
                              date_created,
                              visit_date,
                              test_type,
                              population_type,
                              key_population_type,
                              ever_tested_for_hiv,
                              months_since_last_test,
                              patient_disabled,
                              disability_type,
                              patient_consented,
                              client_tested_as,
                              test_strategy,
                              hts_entry_point,
                              test_1_kit_name,
                              test_1_kit_lot_no,
                              test_1_kit_expiry,
                              test_1_result,
                              test_2_kit_name,
                              test_2_kit_lot_no,
                              test_2_kit_expiry,
                              test_2_result,
                              final_test_result,
                              patient_given_result,
                              couple_discordant,
                              tb_screening,
                              patient_had_hiv_self_test ,
                              remarks,
                              voided
                              )
                          select
                                 e.patient_id,
                                 e.visit_id,
                                 e.encounter_id,
                                 e.uuid,
                                 e.location_id,
                                 e.creator,
                                 e.date_created,
                                 e.encounter_datetime as visit_date,
                                 max(if((o.concept_id=162084 and o.value_coded=162082 and f.uuid = "402dc5d7-46da-42d4-b2be-f43ea4ad87b0") or (f.uuid = "b08471f6-0892-4bf7-ab2b-bf79797b8ea4"), 2, 1)) as test_type , -- 2 for confirmation, 1 for initial
                                 max(if(o.concept_id=164930,(case o.value_coded when 164928 then "General Population" when 164929 then "Key Population" else "" end),null)) as population_type,
                                 max(if(o.concept_id=160581,(case o.value_coded when 105 then "People who inject drugs" when 160578 then "Men who have sex with men" when 160579 then "Female sex worker" else "" end),null)) as key_population_type,
                                 max(if(o.concept_id=164401,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as ever_tested_for_hiv,
                                 max(if(o.concept_id=159813,o.value_numeric,null)) as months_since_last_test,
                                 max(if(o.concept_id=164951,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_disabled,
                                 max(if(o.concept_id=162558,(case o.value_coded when 120291 then "Deaf" when 147215 then "Blind" when 151342 then "Mentally Challenged" when 164538 then "Physically Challenged" when 5622 then "Other" else "" end),null)) as disability_type,
                                 max(if(o.concept_id=1710,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end),null)) as patient_consented,
                                 max(if(o.concept_id=164959,(case o.value_coded when 164957 then "Individual" when 164958 then "Couple" else "" end),null)) as client_tested_as,
                                 max(if(o.concept_id=164956,(
                                     case o.value_coded
                                       when 164163 then "Provider Initiated Testing(PITC)"
                                       when 164953 then "Non Provider Initiated Testing"
                                       when 164954 then "Integrated VCT Center"
                                       when 164955 then "Stand Alone VCT Center"
                                       when 159938 then "Home Based Testing"
                                       when 159939 then "Mobile Outreach HTS"
                                       when 5622 then "Other"
                                       else ""
                                         end ),null)) as test_strategy,
                                 max(if(o.concept_id=160540,(
                                     case o.value_coded
                                       when 5485 then "In Patient Department(IPD)"
                                       when 160542 then "Out Patient Department(OPD)"
                                       when 162181 then "Peadiatric Clinic"
                                       when 160552 then "Nutrition Clinic"
                                       when 160538 then "PMTCT"
                                       when 160541 then "TB"
                                       when 162050 then "CCC"
                                       when 159940 then "VCT"
                                       when 159938 then "Home Based Testing"
                                       when 159939 then "Mobile Outreach"
                                       when 5622 then "Other"
                                       else ""
                                         end ),null)) as hts_entry_point,
                                 max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
                                 max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
                                 max(if(t.test_1_result is not null, t.expiry_date, null)) as test_1_kit_expiry,
                                 max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
                                 max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
                                 max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
                                 max(if(t.test_2_result is not null, t.expiry_date, null)) as test_2_kit_expiry,
                                 max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
                                 max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
                                 max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
                                 max(if(o.concept_id=6096,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as couple_discordant,
                                 max(if(o.concept_id=1659,(case o.value_coded when 1660 then "No TB signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "Not done" when 1111 then "On TB Treatment"  else "" end),null)) as tb_screening,
                                 max(if(o.concept_id=164952,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_had_hiv_self_test,
                                 max(if(o.concept_id=163042,trim(o.value_text),null)) as remarks,
                                 e.voided
                          from openmrs.encounter e
                                 inner join openmrs.form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
                                 inner join openmrs.obs o on o.encounter_id = e.encounter_id and o.concept_id in (162084, 164930, 160581, 164401, 164951, 162558, 1710, 164959, 164956,
                                  160540,159427, 164848, 6096, 1659, 164952, 163042, 159813)
                                 inner join (
                                            select
                                                   o.person_id,
                                                   o.encounter_id,
                                                   o.obs_group_id,
                                                   max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
                                                   max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
                                                   max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
                                                   max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
                                                   max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
                                            from openmrs.obs o
                                                   inner join openmrs.encounter e on e.encounter_id = o.encounter_id
                                                   inner join openmrs.form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
                                            where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
                                            group by e.encounter_id, o.obs_group_id
                                            ) t on e.encounter_id = t.encounter_id
                          group by e.encounter_id;
                          SELECT "Completed processing hts tests";
                          END$$

                          -- ------------------------------------ POPULATE HTS LINKAGES AND REFERRALS -------------------------------

                          DROP PROCEDURE IF EXISTS sp_populate_etl_hts_referral_and_linkage$$
                          CREATE PROCEDURE sp_populate_etl_hts_referral_and_linkage()
                            BEGIN
                              SELECT "Processing hts linkages, referrals and tracing";
                              INSERT INTO kp_etl.etl_hts_referral_and_linkage (
                                  client_id,
                                  visit_id,
                                  encounter_id,
                                  encounter_uuid,
                                  encounter_location,
                                  creator,
                                  date_created,
                                  visit_date,
                                  tracing_type,
                                  tracing_status,
                                  facility_linked_to,
                                  enrollment_date,
                                  art_start_date,
                                  ccc_number,
                                  provider_handed_to,
                                  provider_cadre,
                                  remarks,
                                  voided
                                  )
                              select
                                     e.patient_id,
                                     e.visit_id,
                                     e.encounter_id,
                                     e.uuid,
                                     e.location_id,
                                     e.creator,
                                     e.date_created,
                                     e.encounter_datetime as visit_date,
                                     max(if(o.concept_id=164966,(case o.value_coded when 1650 then "Phone" when 164965 then "Physical" else "" end),null)) as tracing_type ,
                                     max(if(o.concept_id=159811,(case o.value_coded when 1065 then "Contacted and linked" when 1066 then "Contacted but not linked" else "" end),null)) as tracing_status,
                                     max(if(o.concept_id=162724,trim(o.value_text),null)) as facility_linked_to,
                                     max(if(o.concept_id=160555,o.value_datetime,null)) as enrollment_,
                                     max(if(o.concept_id=159599,o.value_datetime,null)) as art_start_,
                                     max(if(o.concept_id=162053,o.value_numeric,null)) as ccc_number,
                                     max(if(o.concept_id=1473,trim(o.value_text),null)) as provider_handed_to,
                                     max(if(o.concept_id=162577,(case o.value_coded when 1577 then "Nurse"
                                                                                    when 1574 then "Clinical Officer/Doctor"
                                                                                    when 1555 then "Community Health Worker"
                                                                                    when 1540 then "Employee"
                                                                                    when 5622 then "Other" else "" end),null)) as provider_cadre,
                                     max(if(o.concept_id=163042,trim(o.value_text),null)),
                                     e.voided
                              from openmrs.encounter e
                                     inner join openmrs.form f on f.form_id = e.form_id and f.uuid = "050a7f12-5c52-4cad-8834-863695af335d"
                                     left outer join openmrs.obs o on o.encounter_id = e.encounter_id and o.concept_id in (164966, 159811, 162724, 160555, 159599, 162053, 1473, 162577, 163042) and o.voided=0
                              group by e.encounter_id;
                              SELECT "Completed processing referral an linkage";
                              END$$

                              -- ------------------------------------ Populat client tracing -------------------------------

                              DROP PROCEDURE IF EXISTS sp_populate_etl_client_tracing$$
                              CREATE PROCEDURE sp_populate_etl_client_tracing()
                                BEGIN
                                  SELECT "Processing hts linkages, referrals and tracing";
                                  INSERT INTO kp_etl.etl_client_tracing (
                                      client_id,
                                      visit_id,
                                      encounter_id,
                                      encounter_uuid,
                                      location_id,
                                      provider,
                                      date_created,
                                      visit_date,
                                      tracing_attempt_date,
                                      tracing_type,
                                      tracing_outcome,
                                      negative_outcome_reason,
                                      negative_outcome_description,
                                      next_tracing_attempt_date,
                                      final_tracing_status,
                                      voided
                                      )
                                  select
                                         e.patient_id,
                                         e.visit_id,
                                         e.encounter_id,
                                         e.uuid,
                                         e.location_id,
                                         e.creator,
                                         e.date_created,
                                         e.encounter_datetime as visit_date,
                                         max(if(o.concept_id=162502,o.value_datetime,null)) as tracing_attempt_date,
                                         max(if(o.concept_id="a55f9516-ddb6-47ec-b10d-cb99d1d0bd41",(case o.value_coded when 1650 then "Phone" when 164965 then "Physical" else "" end),null)) as tracing_type ,
                                         max(if(o.concept_id="eb113c76-aef8-4890-a611-fe22ba003123",(case o.value_coded when 1065 then "Reached" when 1066 then "Not Reached" else "" end),null)) as tracing_outcome,
                                         max(if(o.concept_id="165057",(case o.value_coded
                                                                         when 165060 then "Inaccurate contact details"
                                                                         when 165061 then "Inaccurate location details"
                                                                         when 165058 then "Missing contact details"
                                                                         when 165059 then "Missing location details"
                                                                         when 5622 then "Other"
                                                                         else "" end),null)) as negative_outcome_reason,
                                         max(if(o.concept_id=163042,trim(o.value_text),null)) as negative_outcome_description,
                                         max(if(o.concept_id=162502,o.value_datetime,null)) as next_tracing_attempt_date,
                                         max(if(o.concept_id=165054,(case o.value_coded
                                                                       when 165048 then "Self Transfer"
                                                                       when 164349 then "Treatment interrupted/stopped"
                                                                       when 165049 then "Willing to return to services"
                                                                       when 160415 then "Moved away"
                                                                       when 165050 then "Moved to another hotspot"
                                                                       when 165051 then "Not happy with the program"
                                                                       when 165052 then "Stopped sex work and/or injecting drugs"
                                                                       when 162277 then "In prison"
                                                                       when 160432 then "Dead"
                                                                       when 165053 then "Untraceable"
                                                                       else "" end),null)) as final_tracing_status,
                                         e.voided
                                  from openmrs.encounter e
                                         inner join openmrs.form f on f.form_id = e.form_id and f.uuid = "050a7f12-5c52-4cad-8834-863695af335d"
                                         left outer join openmrs.obs o on o.encounter_id = e.encounter_id and o.concept_id in (162502, 165057,163042, 162502, 165054) and o.voided=0
                                  group by e.encounter_id;
                                  SELECT "Completed processing Client tracing";
                                  END$$



-- ------------------------------------------- running all procedures -----------------------------

DROP PROCEDURE IF EXISTS sp_first_time_setup$$
CREATE PROCEDURE sp_first_time_setup()
BEGIN
DECLARE populate_script_id INT(11);
SELECT "Beginning first time setup", CONCAT("Time: ", NOW());
INSERT INTO kP_etl.etl_script_status(script_name, start_time) VALUES('initial_population_of_tables', NOW());
SET populate_script_id = LAST_INSERT_ID();

CALL sp_populate_etl_client_registration();
CALL sp_populate_etl_client_social_status();
CALL sp_populate_etl_client_enrollment();
CALL sp_populate_etl_client_complaints();
CALL sp_populate_etl_chronic_illness();
CALL sp_populate_etl_allergies();
CALL sp_populate_etl_pregnancy_fp_cacx_screening();
CALL sp_populate_etl_adverse_drug_reaction();
CALL sp_populate_etl_immunization_screening();
CALL sp_populate_etl_hepatitis_screening();
CALL sp_populate_etl_tb_screening();
CALL sp_populate_etl_systems_review();
CALL sp_populate_etl_diagnosis_treatment();
CALL sp_populate_etl_clinical_notes();
CALL sp_populate_etl_appointment();
CALL sp_populate_etl_alcohol_drugs_risk_screening();
CALL sp_populate_etl_violence_screening();
CALL sp_populate_counselling_services();
CALL sp_populate_etl_prep_pep_screening();
CALL sp_populate_hts_test();
CALL sp_populate_etl_hts_referral_and_linkage();
CALL sp_populate_etl_client_tracing();


UPDATE kp_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed first time setup", CONCAT("Time: ", NOW());
END$$




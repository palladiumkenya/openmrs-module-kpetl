
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

-- up etl_client_registration with patient attributes: birthplace, citizenship, mother_name, phone number and kin's details
up kp_etl.etl_client_registration r
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
CREATE PROCEDURE sp_populate_etl_client_triage()
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
            SELECT "Processing chronic illness ", CONCAT("Time: ", NOW());
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
                       max(if(o.concept_id=165067,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as eligible_for_fp,
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
                       max(if(o.concept_id=165069,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as referred_for_fp,
                       max(if(o.concept_id=164934,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as cacx_screening,
                       max(if(o.concept_id=165026,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as cacx_screening_results,
                       max(if(o.concept_id=165038,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as treated,
                       max(if(o.concept_id=1272,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as referred,
                       e.voided as voided
                from encounter e
                       inner join
                         (
                         select encounter_type_id, uuid, name from encounter_type where uuid in('55d0b03e-8977-4d3e-8941-3333712b1afe')
                         ) et on et.encounter_type_id=e.encounter_type
                       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                                  and o.concept_id in (1427,5272,5596,160653,165067,374,165069,164934,165026,165038,1272)
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

                    DROP PROCEDURE IF EXISTS etl_immunization_screening$$
                    CREATE PROCEDURE sp_populate_etl_adverse_drug_reaction()
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

                                    DROP PROCEDURE IF EXISTS etl_systems_review$$
                                    CREATE PROCEDURE sp_populate_etl_systems_review$$()
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
                                                SELECT "Completed processing TB screening data ", CONCAT("Time: ", NOW());
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
                                                                        



















DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_enrollment$$
CREATE PROCEDURE sp_populate_etl_hiv_enrollment()
BEGIN
-- populate patient_hiv_enrollment table
-- uuid: de78a6be-bfc5-4634-adc3-5f1a280455cc
SELECT "Processing KP Enrollment data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_hiv_enrollment (
patient_id,
uuid,
visit_id,
visit_,
location_id,
encounter_id,
encounter_provider,
_created,
patient_type,
_first_enrolled_in_care,
entry_point,
transfer_in_,
facility_transferred_from,
district_transferred_from,
_started_art_at_transferring_facility,
_confirmed_hiv_positive,
facility_confirmed_hiv_positive,
arv_status,
name_of_treatment_supporter,
relationship_of_treatment_supporter,
treatment_supporter_telephone,
treatment_supporter_address,
voided
)
select
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_time as visit_,
e.location_id,
e.encounter_id,
e.creator,
e._created,
max(if(o.concept_id in (164932), o.value_coded, if(o.concept_id=160563 and o.value_coded=1065, 160563, null))) as patient_type ,
max(if(o.concept_id=160555,o.value_time,null)) as _first_enrolled_in_care ,
max(if(o.concept_id=160540,o.value_coded,null)) as entry_point,
max(if(o.concept_id=160534,o.value_time,null)) as transfer_in_,
max(if(o.concept_id=160535,left(trim(o.value_text),100),null)) as facility_transferred_from,
max(if(o.concept_id=161551,left(trim(o.value_text),100),null)) as district_transferred_from,
max(if(o.concept_id=159599,o.value_time,null)) as _started_art_at_transferring_facility,
max(if(o.concept_id=160554,o.value_time,null)) as _confirmed_hiv_positive,
max(if(o.concept_id=160632,left(trim(o.value_text),100),null)) as facility_confirmed_hiv_positive,
max(if(o.concept_id=160533,o.value_boolean,null)) as arv_status,
max(if(o.concept_id=160638,left(trim(o.value_text),100),null)) as name_of_treatment_supporter,
max(if(o.concept_id=160640,o.value_coded,null)) as relationship_of_treatment_supporter,
max(if(o.concept_id=160642,left(trim(o.value_text),100),null)) as treatment_supporter_telephone ,
max(if(o.concept_id=160641,left(trim(o.value_text),100),null)) as treatment_supporter_address,
e.voided
from encounter e
inner join
(
	select encounter_type_id, uuid, name from encounter_type where uuid='de78a6be-bfc5-4634-adc3-5f1a280455cc'
) et on et.encounter_type_id=e.encounter_type
join patient p on p.patient_id=e.patient_id and p.voided=0
left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
	and o.concept_id in (160555,160540,160534,160535,161551,159599,160554,160632,160533,160638,160640,160642,160641,164932,160563)
where e.voided=0
group by e.patient_id, e.encounter_id;
SELECT "Completed processing HIV Enrollment data ", CONCAT("Time: ", NOW());
END$$



-- ------------- populate etl_hiv_followup--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_followup$$
CREATE PROCEDURE sp_populate_etl_hiv_followup()
BEGIN
SELECT "Processing HIV Followup data ", CONCAT("Time: ", NOW());
INSERT INTO kenyaemr_etl.etl_patient_hiv_followup(
patient_id,
visit_id,
visit_,
location_id,
encounter_id,
encounter_provider,
_created,
visit_scheduled,
person_present,
weight,
systolic_pressure,
diastolic_pressure,
height,
temperature,
pulse_rate,
respiratory_rate,
oxygen_saturation,
muac,
nutritional_status,
population_type,
key_population_type,
who_stage,
presenting_complaints,
clinical_notes,
on_anti_tb_drugs,
on_ipt,
ever_on_ipt,
spatum_smear_ordered,
chest_xray_ordered,
genexpert_ordered,
spatum_smear_result,
chest_xray_result,
genexpert_result,
referral,
clinical_tb_diagnosis,
contact_invitation,
evaluated_for_ipt,
has_known_allergies,
has_chronic_illnesses_cormobidities,
has_adverse_drug_reaction,
pregnancy_status,
wants_pregnancy,
pregnancy_outcome,
anc_number,
expected_delivery_,
last_menstrual_period,
gravida,
parity,
full_term_pregnancies,
abortion_miscarriages,
family_planning_status,
family_planning_method,
reason_not_using_family_planning,
tb_status,
tb_treatment_no,
ctx_adherence,
ctx_dispensed,
dapsone_adherence,
dapsone_dispensed,
inh_dispensed,
arv_adherence,
poor_arv_adherence_reason,
poor_arv_adherence_reason_other,
pwp_disclosure,
pwp_partner_tested,
condom_provided,
screened_for_sti,
cacx_screening,
sti_partner_notification,
at_risk_population,
system_review_finding,
next_appointment_,
next_appointment_reason,
stability,
differentiated_care,
voided
)
select
e.patient_id,
e.visit_id,
(e.encounter_time) as visit_,
e.location_id,
e.encounter_id as encounter_id,
e.creator,
e._created as _created,
max(if(o.concept_id=1246,o.value_coded,null)) as visit_scheduled ,
max(if(o.concept_id=161643,o.value_coded,null)) as person_present,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_pressure,
max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_pressure,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
max(if(o.concept_id=163300,o.value_coded,null)) as nutritional_status,
max(if(o.concept_id=164930,o.value_coded,null)) as population_type,
max(if(o.concept_id=160581,o.value_coded,null)) as key_population_type,
max(if(o.concept_id=5356,o.value_coded,null)) as who_stage ,
max(if(o.concept_id=1154,o.value_coded,null)) as presenting_complaints ,
null as clinical_notes, -- max(if(o.concept_id=160430,left(trim(o.value_text),600),null)) as clinical_notes ,
max(if(o.concept_id=164948,o.value_coded,null)) as on_anti_tb_drugs ,
max(if(o.concept_id=164949,o.value_coded,null)) as on_ipt ,
max(if(o.concept_id=164950,o.value_coded,null)) as ever_on_ipt ,
max(if(o.concept_id=1271 and o.value_coded = 307,1065,1066)) as spatum_smear_ordered ,
max(if(o.concept_id=1271 and o.value_coded = 12 ,1065,1066)) as chest_xray_ordered ,
max(if(o.concept_id=1271 and o.value_coded = 162202,1065,1066)) as genexpert_ordered ,
max(if(o.concept_id=307,o.value_coded,null)) as spatum_smear_result ,
max(if(o.concept_id=12,o.value_coded,null)) as chest_xray_result ,
max(if(o.concept_id=162202,o.value_coded,null)) as genexpert_result ,
max(if(o.concept_id=1272,o.value_coded,null)) as referral ,
max(if(o.concept_id=163752,o.value_coded,null)) as clinical_tb_diagnosis ,
max(if(o.concept_id=163414,o.value_coded,null)) as contact_invitation ,
max(if(o.concept_id=162275,o.value_coded,null)) as evaluated_for_ipt ,
max(if(o.concept_id=160557,o.value_coded,null)) as has_known_allergies ,
max(if(o.concept_id=162747,o.value_coded,null)) as has_chronic_illnesses_cormobidities ,
max(if(o.concept_id=121764,o.value_coded,null)) as has_adverse_drug_reaction ,
max(if(o.concept_id=5272,o.value_coded,null)) as pregnancy_status,
max(if(o.concept_id=164933,o.value_coded,null)) as wants_pregnancy,
max(if(o.concept_id=161033,o.value_coded,null)) as pregnancy_outcome,
max(if(o.concept_id=163530,o.value_text,null)) as anc_number,
max(if(o.concept_id=5596,(o.value_time),null)) as expected_delivery_,
max(if(o.concept_id=1427,(o.value_time),null)) as last_menstrual_period,
max(if(o.concept_id=5624,o.value_numeric,null)) as gravida,
max(if(o.concept_id=1053,o.value_numeric,null)) as parity ,
max(if(o.concept_id=160080,o.value_numeric,null)) as full_term_pregnancies,
max(if(o.concept_id=1823,o.value_numeric,null)) as abortion_miscarriages ,
max(if(o.concept_id=160653,o.value_coded,null)) as family_planning_status,
max(if(o.concept_id=374,o.value_coded,null)) as family_planning_method,
max(if(o.concept_id=160575,o.value_coded,null)) as reason_not_using_family_planning ,
max(if(o.concept_id=1659,o.value_coded,null)) as tb_status,
max(if(o.concept_id=161654,trim(o.value_text),null)) as tb_treatment_no,
max(if(o.concept_id=161652,o.value_coded,null)) as ctx_adherence,
max(if(o.concept_id=162229 or (o.concept_id=1282 and o.value_coded = 105281),o.value_coded,null)) as ctx_dispensed,
max(if(o.concept_id=164941,o.value_coded,null)) as dapsone_adherence,
max(if(o.concept_id=164940 or (o.concept_id=1282 and o.value_coded = 74250),o.value_coded,null)) as dapsone_dispensed,
max(if(o.concept_id=162230,o.value_coded,null)) as inh_dispensed,
max(if(o.concept_id=1658,o.value_coded,null)) as arv_adherence,
max(if(o.concept_id=160582,o.value_coded,null)) as poor_arv_adherence_reason,
null as poor_arv_adherence_reason_other, -- max(if(o.concept_id=160632,trim(o.value_text),null)) as poor_arv_adherence_reason_other,
max(if(o.concept_id=159423,o.value_coded,null)) as pwp_disclosure,
max(if(o.concept_id=161557,o.value_coded,null)) as pwp_partner_tested,
max(if(o.concept_id=159777,o.value_coded,null)) as condom_provided ,
max(if(o.concept_id=161558,o.value_coded,null)) as screened_for_sti,
max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
max(if(o.concept_id=164935,o.value_coded,null)) as sti_partner_notification,
max(if(o.concept_id=160581,o.value_coded,null)) as at_risk_population,
max(if(o.concept_id=159615,o.value_coded,null)) as system_review_finding,
max(if(o.concept_id=5096,o.value_time,null)) as next_appointment_,
max(if(o.concept_id=160288,o.value_coded,null)) as next_appointment_reason,
max(if(o.concept_id=1855,o.value_coded,null)) as stability,
max(if(o.concept_id=164947,o.value_coded,null)) as differentiated_care,
e.voided as voided
from encounter e
inner join
(
	select encounter_type_id, uuid, name from encounter_type where uuid in('a0034eee-1940-4e35-847f-97537a35d05e','d1059fb9-a079-4feb-a749-eedd709ae542', '465a92f2-baf8-42e9-9612-53064be868e8')
) et on et.encounter_type_id=e.encounter_type
left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
	and o.concept_id in (1282,1246,161643,5089,5085,5086,5090,5088,5087,5242,5092,1343,5356,5272,161033,163530,5596,1427,5624,1053,160653,374,160575,1659,161654,161652,162229,162230,1658,160582,160632,159423,161557,159777,161558,160581,5096,163300, 164930, 160581, 1154, 160430, 164948, 164949, 164950, 1271, 307, 12, 162202, 1272, 163752, 163414, 162275, 160557, 162747,
121764, 164933, 160080, 1823, 164940, 164934, 164935, 159615, 160288, 1855, 164947)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_
;
SELECT "Completed processing HIV Followup data ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_laboratory_extract  uuid:  --------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_laboratory_extract$$
CREATE PROCEDURE sp_populate_etl_laboratory_extract()
BEGIN
SELECT "Processing Laboratory data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_laboratory_extract(
uuid,
encounter_id,
patient_id,
location_id,
visit_,
visit_id,
lab_test,
urgency,
test_result,
-- _test_requested,
-- _test_result_received,
-- test_requested_by,
_created,
created_by
)
select
o.uuid,
e.encounter_id,
e.patient_id,
e.location_id,
e.encounter_time as visit_,
e.visit_id,
o.concept_id,
od.urgency,
(CASE when o.concept_id in(5497,730,654,790,856) then o.value_numeric
	when o.concept_id in(1030,1305) then o.value_coded
	END) AS test_result,
--  requested,
--  result received
-- test requested by
e._created,
e.creator
from encounter e
inner join
(
	select encounter_type_id, uuid, name from encounter_type where uuid in('17a381d1-7e29-406a-b782-aa903b963c28', 'a0034eee-1940-4e35-847f-97537a35d05e','e1406e88-e9a9-11e8-9f32-f2801f1b9fd1', 'de78a6be-bfc5-4634-adc3-5f1a280455cc')
) et on et.encounter_type_id=e.encounter_type
inner join obs o on e.encounter_id=o.encounter_id and o.voided=0 and o.concept_id in (5497,730,654,790,856,1030,1305)
left join orders od on od.order_id = o.order_id and od.voided=0
;

/*-- >>>>>>>>>>>>>>> -----------------------------------  Wagners input ------------------------------------------------------------
insert into kenyaemr_etl.etl_laboratory_extract(
encounter_id,
patient_id,
visit_,
visit_id,
lab_test,
test_result,
-- _test_requested,
-- _test_result_received,
-- test_requested_by,
_created,
created_by
)
select
e.encounter_id,
e.patient_id,
e.encounter_time as visit_,
e.visit_id,
o.concept_id,
(CASE when o.concept_id in(5497,730,654,790,856,21) then o.value_numeric
when o.concept_id in(299,1030,302,32) then o.value_coded
END) AS test_result,
--  requested,
--  result received
-- test requested by
e._created,
e.creator
from encounter e, obs o, encounter_type et
where e.encounter_id=o.encounter_id and o.voided=0
and o.concept_id in (5497,730,299,654,790,856,1030,21,302,32) and et.encounter_type_id=e.encounter_type
group by e.encounter_id;

-- --------<<<<<<<<<<<<<<<<<<<< ------------------------------------------------------------------------------------------------------
*/
SELECT "Completed processing Laboratory data ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_pharmacy_extract table--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_pharmacy_extract$$
CREATE PROCEDURE sp_populate_etl_pharmacy_extract()
BEGIN
SELECT "Processing Pharmacy data ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_pharmacy_extract(
obs_group_id,
patient_id,
uuid,
visit_,
visit_id,
encounter_id,
_created,
encounter_name,
location_id,
drug,
drug_name,
is_arv,
is_ctx,
is_dapsone,
frequency,
duration,
duration_units,
voided,
_voided,
dispensing_provider
)
select
	o.obs_group_id obs_group_id,
	o.person_id,
	max(if(o.concept_id=1282, o.uuid, null)),
	(o.obs_time) as enc_,
	e.visit_id,
	o.encounter_id,
	e._created,
	et.name as enc_name,
	e.location_id,
	max(if(o.concept_id = 1282 and o.value_coded is not null,o.value_coded, null)) as drug_dispensed,
	max(if(o.concept_id = 1282, left(cn.name,255), 0)) as drug_name, -- arv:1085
	max(if(o.concept_id = 1282 and cs.concept_set=1085, 1, 0)) as arv_drug, -- arv:1085
	max(if(o.concept_id = 1282 and o.value_coded = 105281,1, 0)) as is_ctx,
	max(if(o.concept_id = 1282 and o.value_coded = 74250,1, 0)) as is_dapsone,
	max(if(o.concept_id = 1443, o.value_numeric, null)) as dose,
	max(if(o.concept_id = 159368, if(o.value_numeric > 10000, 10000, o.value_numeric), null)) as duration, -- catching typos in duration field
	max(if(o.concept_id = 1732 and o.value_coded=1072,'Days',if(o.concept_id=1732 and o.value_coded=1073,'Weeks',if(o.concept_id=1732 and o.value_coded=1074,'Months',null)))) as duration_units,
	o.voided,
	o._voided,
	e.creator
from obs o
left outer join encounter e on e.encounter_id = o.encounter_id and e.voided=0
left outer join encounter_type et on et.encounter_type_id = e.encounter_type
left outer join concept_name cn on o.value_coded = cn.concept_id and cn.locale='en' and cn.concept_name_type='FULLY_SPECIFIED' -- SHORT'
left outer join concept_set cs on o.value_coded = cs.concept_id
where o.voided=0 and o.concept_id in(1282,1732,159368,1443,1444)  and e.voided=0
group by o.obs_group_id, o.person_id, encounter_id
having drug_dispensed is not null and obs_group_id is not null;

up kenyaemr_etl.etl_pharmacy_extract
	set duration_in_days = if(duration_units= 'Days', duration,if(duration_units='Weeks',duration * 7,if(duration_units='Months',duration * 31,null)))
	where (duration is not null or duration <> "") and (duration_units is not null or duration_units <> "");

SELECT "Completed processing Pharmacy data ", CONCAT("Time: ", NOW());
END$$


-- ------------ create table etl_patient_treatment_event----------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_program_discontinuation$$
CREATE PROCEDURE sp_populate_etl_program_discontinuation()
BEGIN
SELECT "Processing Program (HIV, TB, MCH ...) discontinuations ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_patient_program_discontinuation(
patient_id,
uuid,
visit_id,
visit_,
program_uuid,
program_name,
encounter_id,
discontinuation_reason,
_died,
transfer_facility,
transfer_
)
select
e.patient_id,
e.uuid,
e.visit_id,
e.encounter_time, -- trying to make us of index
et.uuid,
(case et.uuid
	when '2bdada65-4c72-4a48-8730-859890e25cee' then 'HIV'
	when 'd3e3d723-7458-4b4e-8998-408e8a551a84' then 'TB'
	when '01894f88-dc73-42d4-97a3-0929118403fb' then 'MCH Child HEI'
	when '5feee3f1-aa16-4513-8bd0-5d9b27ef1208' then 'MCH Child'
	when '7c426cfc-3b47-4481-b55f-89860c21c7de' then 'MCH Mother'
end) as program_name,
e.encounter_id,
max(if(o.concept_id=161555, o.value_coded, null)) as reason_discontinued,
max(if(o.concept_id=1543, o.value_time, null)) as _died,
max(if(o.concept_id=159495, left(trim(o.value_text),100), null)) as to_facility,
max(if(o.concept_id=160649, o.value_time, null)) as to_
from encounter e
inner join obs o on o.encounter_id=e.encounter_id and o.voided=0 and o.concept_id in (161555,1543,159495,160649)
inner join
(
	select encounter_type_id, uuid, name from encounter_type where
	uuid in('2bdada65-4c72-4a48-8730-859890e25cee','d3e3d723-7458-4b4e-8998-408e8a551a84','5feee3f1-aa16-4513-8bd0-5d9b27ef1208','7c426cfc-3b47-4481-b55f-89860c21c7de','01894f88-dc73-42d4-97a3-0929118403fb')
) et on et.encounter_type_id=e.encounter_type
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing discontinuation data ", CONCAT("Time: ", NOW());
END$$

-- ------------- populate etl_mch_enrollment-------------------------
DROP PROCEDURE IF EXISTS sp_populate_etl_mch_enrollment$$
CREATE PROCEDURE sp_populate_etl_mch_enrollment()
	BEGIN
		SELECT "Processing MCH Enrollments ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mch_enrollment(
			patient_id,
			uuid,
			visit_id,
			visit_,
			location_id,
			encounter_id,
			anc_number,
			first_anc_visit_,
			gravida,
			parity,
			parity_abortion,
			age_at_menarche,
			lmp,
			lmp_estimated,
			edd_ultrasound,
			blood_group,
			serology,
			tb_screening,
			bs_for_mps,
			hiv_status,
			hiv_test_,
			partner_hiv_status,
			partner_hiv_test_,
			urine_microscopy,
			urinary_albumin,
			glucose_measurement,
			urine_ph,
			urine_gravity,
			urine_nitrite_test,
			urine_leukocyte_esterace_test,
			urinary_ketone,
			urine_bile_salt_test,
			urine_bile_pigment_test,
			urine_colour,
			urine_turbidity,
			urine_dipstick_for_blood,
			-- _of_discontinuation,
			discontinuation_reason
		)
			select
				e.patient_id,
				e.uuid,
				e.visit_id,
				e.encounter_time,
				e.location_id,
				e.encounter_id,
				max(if(o.concept_id=163530,o.value_text,null)) as anc_number,
				max(if(o.concept_id=163547,o.value_time,null)) as first_anc_visit_,
				max(if(o.concept_id=5624,o.value_numeric,null)) as gravida,
				max(if(o.concept_id=160080,o.value_numeric,null)) as parity,
				max(if(o.concept_id=1823,o.value_numeric,null)) as parity_abortion,
				max(if(o.concept_id=160598,o.value_numeric,null)) as age_at_menarche,
				max(if(o.concept_id=1427,o.value_time,null)) as lmp,
				max(if(o.concept_id=162095,o.value_time,null)) as lmp_estimated,
				max(if(o.concept_id=5596,o.value_time,null)) as edd_ultrasound,
				max(if(o.concept_id=300,o.value_coded,null)) as blood_group,
				max(if(o.concept_id=299,o.value_coded,null)) as serology,
				max(if(o.concept_id=160108,o.value_coded,null)) as tb_screening,
				max(if(o.concept_id=32,o.value_coded,null)) as bs_for_mps,
				max(if(o.concept_id=159427,o.value_coded,null)) as hiv_status,
				max(if(o.concept_id=160554,o.value_time,null)) as hiv_test_,
				max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
				max(if(o.concept_id=160082,o.value_time,null)) as partner_hiv_test_,
				max(if(o.concept_id=56,o.value_text,null)) as urine_microscopy,
				max(if(o.concept_id=1875,o.value_coded,null)) as urinary_albumin,
				max(if(o.concept_id=159734,o.value_coded,null)) as glucose_measurement,
				max(if(o.concept_id=161438,o.value_numeric,null)) as urine_ph,
				max(if(o.concept_id=161439,o.value_numeric,null)) as urine_gravity,
				max(if(o.concept_id=161440,o.value_coded,null)) as urine_nitrite_test,
				max(if(o.concept_id=161441,o.value_coded,null)) as urine_leukocyte_esterace_test,
				max(if(o.concept_id=161442,o.value_coded,null)) as urinary_ketone,
				max(if(o.concept_id=161444,o.value_coded,null)) as urine_bile_salt_test,
				max(if(o.concept_id=161443,o.value_coded,null)) as urine_bile_pigment_test,
				max(if(o.concept_id=162106,o.value_coded,null)) as urine_colour,
				max(if(o.concept_id=162101,o.value_coded,null)) as urine_turbidity,
				max(if(o.concept_id=162096,o.value_coded,null)) as urine_dipstick_for_blood,
				-- max(if(o.concept_id=161655,o.value_text,null)) as _of_discontinuation,
				max(if(o.concept_id=161555,o.value_coded,null)) as discontinuation_reason
			from encounter e
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(163530,163547,5624,160080,1823,160598,1427,162095,5596,300,299,160108,32,159427,160554,1436,160082,56,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,161555)
				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where
						uuid in('3ee036d8-7c13-4393-b5d6-036f2fe45126')
				) et on et.encounter_type_id=e.encounter_type
			group by e.encounter_id;
		SELECT "Completed processing MCH Enrollments ", CONCAT("Time: ", NOW());
		END$$
-- ------------- populate etl_mch_antenatal_visit-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_antenatal_visit$$
CREATE PROCEDURE sp_populate_etl_mch_antenatal_visit()
	BEGIN
		SELECT "Processing MCH antenatal visits ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mch_antenatal_visit(
			patient_id,
			uuid,
			visit_id,
			visit_,
			location_id,
			encounter_id,
			provider,
			anc_visit_number,
			temperature,
			pulse_rate,
			systolic_bp,
			diastolic_bp,
			respiratory_rate,
			oxygen_saturation,
			weight,
			height,
			muac,
			hemoglobin,
			breast_exam_done,
			pallor,
			maturity,
			fundal_height,
			fetal_presentation,
			lie,
			fetal_heart_rate,
			fetal_movement,
			who_stage,
			cd4,
			viral_load,
			ldl,
			arv_status,
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
			partner_hiv_tested,
			partner_hiv_status,
			prophylaxis_given,
			baby_azt_dispensed,
			baby_nvp_dispensed,
			TTT,
			IPT_malaria,
			iron_supplement,
			deworming,
			bed_nets,
			urine_microscopy,
			urinary_albumin,
			glucose_measurement,
			urine_ph,
			urine_gravity,
			urine_nitrite_test,
			urine_leukocyte_esterace_test,
			urinary_ketone,
			urine_bile_salt_test,
			urine_bile_pigment_test,
			urine_colour,
			urine_turbidity,
			urine_dipstick_for_blood,
			syphilis_test_status,
			syphilis_treated_status,
			bs_mps,
			anc_exercises,
			tb_screening,
			cacx_screening,
			cacx_screening_method,
			has_other_illnes,
			counselled,
			referred_from,
			referred_to,
			next_appointment_,
			clinical_notes
		)
			select
				e.patient_id,
				e.uuid,
				e.visit_id,
				e.encounter_time,
				e.location_id,
				e.encounter_id,
				e.creator,
				max(if(o.concept_id=1425,o.value_numeric,null)) as anc_visit_number,
				max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
				max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
				max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_bp,
				max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_bp,
				max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
				max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=5090,o.value_numeric,null)) as height,
				max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
				max(if(o.concept_id=21,o.value_numeric,null)) as hemoglobin,
				max(if(o.concept_id=163590,o.value_coded,null)) as breast_exam_done,
				max(if(o.concept_id=5245,o.value_coded,null)) as pallor,
				max(if(o.concept_id=1438,o.value_numeric,null)) as maturity,
				max(if(o.concept_id=1439,o.value_numeric,null)) as fundal_height,
				max(if(o.concept_id=160090,o.value_coded,null)) as fetal_presentation,
				max(if(o.concept_id=162089,o.value_coded,null)) as lie,
				max(if(o.concept_id=1440,o.value_numeric,null)) as fetal_heart_rate,
				max(if(o.concept_id=162107,o.value_coded,null)) as fetal_movement,
				max(if(o.concept_id=5356,o.value_coded,null)) as who_stage,
				max(if(o.concept_id=5497,o.value_numeric,null)) as cd4,
				max(if(o.concept_id=856,o.value_numeric,null)) as viral_load,
				max(if(o.concept_id=1305,o.value_coded,null)) as ldl,
				max(if(o.concept_id=1147,o.value_coded,null)) as arv_status,
				max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
				max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
				max(if(t.test_1_result is not null, t.expiry_, null)) as test_1_kit_expiry,
				max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
				max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
				max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
				max(if(t.test_2_result is not null, t.expiry_, null)) as test_2_kit_expiry,
				max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
				max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
				max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
				max(if(o.concept_id=161557,o.value_coded,null)) as partner_hiv_tested,
				max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
				max(if(o.concept_id=1109,o.value_coded,null)) as prophylaxis_given,
				max(if(o.concept_id=1282,o.value_coded,null)) as baby_azt_dispensed,
				max(if(o.concept_id=1282,o.value_coded,null)) as baby_nvp_dispensed,
				max(if(o.concept_id=984,(case o.value_coded when 84879 then "Yes" else "" end),null)) as TTT,
				max(if(o.concept_id=984,(case o.value_coded when 159610 then "Yes" else "" end),null)) as IPT_malaria,
				max(if(o.concept_id=984,(case o.value_coded when 104677 then "Yes" else "" end),null)) as iron_supplement,
				max(if(o.concept_id=984,(case o.value_coded when 79413 then "Yes"  else "" end),null)) as deworming,
				max(if(o.concept_id=984,(case o.value_coded when 160428 then "Yes" else "" end),null)) as bed_nets,
				max(if(o.concept_id=56,o.value_text,null)) as urine_microscopy,
				max(if(o.concept_id=1875,o.value_coded,null)) as urinary_albumin,
				max(if(o.concept_id=159734,o.value_coded,null)) as glucose_measurement,
				max(if(o.concept_id=161438,o.value_numeric,null)) as urine_ph,
				max(if(o.concept_id=161439,o.value_numeric,null)) as urine_gravity,
				max(if(o.concept_id=161440,o.value_coded,null)) as urine_nitrite_test,
				max(if(o.concept_id=161441,o.value_coded,null)) as urine_leukocyte_esterace_test,
				max(if(o.concept_id=161442,o.value_coded,null)) as urinary_ketone,
				max(if(o.concept_id=161444,o.value_coded,null)) as urine_bile_salt_test,
				max(if(o.concept_id=161443,o.value_coded,null)) as urine_bile_pigment_test,
				max(if(o.concept_id=162106,o.value_coded,null)) as urine_colour,
				max(if(o.concept_id=162101,o.value_coded,null)) as urine_turbidity,
				max(if(o.concept_id=162096,o.value_coded,null)) as urine_dipstick_for_blood,
				max(if(o.concept_id=299,o.value_coded,null)) as syphilis_test_status,
				max(if(o.concept_id=159918,o.value_coded,null)) as syphilis_treated_status,
				max(if(o.concept_id=32,o.value_coded,null)) as bs_mps,
				max(if(o.concept_id=161074,o.value_coded,null)) as anc_exercises,
				max(if(o.concept_id=1659,o.value_coded,null)) as tb_screening,
				max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
				max(if(o.concept_id=163589,o.value_coded,null)) as cacx_screening_method,
				max(if(o.concept_id=162747,o.value_coded,null)) as has_other_illnes,
				max(if(o.concept_id=1912,o.value_coded,null)) as counselled,
				max(if(o.concept_id=160481,o.value_coded,null)) as referred_from,
				max(if(o.concept_id=163145,o.value_coded,null)) as referred_to,
				max(if(o.concept_id=5096,o.value_time,null)) as next_appointment_,
				max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes

			from encounter e
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(1282,984,1425,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,163590,5245,1438,1439,160090,162089,1440,162107,5356,5497,856,1305,1147,159427,164848,161557,1436,1109,128256,1875,159734,161438,161439,161440,161441,161442,161444,161443,162106,162101,162096,299,159918,32,161074,1659,164934,163589,162747,1912,160481,163145,5096,159395)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('e8f98494-af35-4bb8-9fc7-c409c8fed843','d3ea25c7-a3e8-4f57-a6a9-e802c3565a30')
				) f on f.form_id=e.form_id
				left join (
										 select
											 o.person_id,
											 o.encounter_id,
											 o.obs_group_id,
											 max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
											 max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
											 max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
											 max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
											 max(if(o.concept_id=162502,(o.value_time),null)) as expiry_
										 from obs o
											 inner join encounter e on e.encounter_id = o.encounter_id
											 inner join form f on f.form_id=e.form_id and f.uuid in ('e8f98494-af35-4bb8-9fc7-c409c8fed843')
										 where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
										 group by e.encounter_id, o.obs_group_id
									 ) t on e.encounter_id = t.encounter_id

			group by e.encounter_id;
		SELECT "Completed processing MCH antenatal visits ", CONCAT("Time: ", NOW());
		END$$


-- ------------- populate etl_mchs_delivery-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_delivery$$
CREATE PROCEDURE sp_populate_etl_mch_delivery()
	BEGIN
		SELECT "Processing MCH Delivery visits", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mchs_delivery(
			patient_id,
			uuid,
			provider,
			visit_id,
			visit_,
			location_id,
			encounter_id,
			_created,
			admission_number,
			duration_of_pregnancy,
			mode_of_delivery,
			_of_delivery,
			blood_loss,
			condition_of_mother,
			apgar_score_1min,
			apgar_score_5min,
			apgar_score_10min,
			resuscitation_done,
			place_of_delivery,
			delivery_assistant,
			counseling_on_infant_feeding ,
			counseling_on_exclusive_breastfeeding,
			counseling_on_infant_feeding_for_hiv_infected,
			mother_decision,
			placenta_complete,
			maternal_death_audited,
			cadre,
			delivery_complications,
			coded_delivery_complications,
			other_delivery_complications,
			duration_of_labor,
			baby_sex,
			baby_condition,
			teo_given,
			birth_weight,
			bf_within_one_hour,
			birth_with_deformity,
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
			partner_hiv_tested,
			partner_hiv_status,
			prophylaxis_given,
			baby_azt_dispensed,
			baby_nvp_dispensed,
			clinical_notes
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.visit_id,
				e.encounter_time,
				e.location_id,
				e.encounter_id,
				e._created,
				max(if(o.concept_id=162054,o.value_text,null)) as admission_number,
				max(if(o.concept_id=1789,o.value_numeric,null)) as duration_of_pregnancy,
				max(if(o.concept_id=5630,o.value_coded,null)) as mode_of_delivery,
				max(if(o.concept_id=5599,o.value_time,null)) as _of_delivery,
				max(if(o.concept_id=162092,o.value_coded,null)) as blood_loss,
				max(if(o.concept_id=1856,o.value_coded,null)) as condition_of_mother,
				max(if(o.concept_id=159603,o.value_numeric,null)) as apgar_score_1min,
				max(if(o.concept_id=159604,o.value_numeric,null)) as apgar_score_5min,
				max(if(o.concept_id=159605,o.value_numeric,null)) as apgar_score_10min,
				max(if(o.concept_id=162131,o.value_coded,null)) as resuscitation_done,
				max(if(o.concept_id=1572,o.value_coded,null)) as place_of_delivery,
				max(if(o.concept_id=1473,o.value_text,null)) as delivery_assistant,
				max(if(o.concept_id=1379 and o.value_coded=161651,o.value_coded,null)) as counseling_on_infant_feeding,
				max(if(o.concept_id=1379 and o.value_coded=161096,o.value_coded,null)) as counseling_on_exclusive_breastfeeding,
				max(if(o.concept_id=1379 and o.value_coded=162091,o.value_coded,null)) as counseling_on_infant_feeding_for_hiv_infected,
				max(if(o.concept_id=1151,o.value_coded,null)) as mother_decision,
				max(if(o.concept_id=163454,o.value_coded,null)) as placenta_complete,
				max(if(o.concept_id=1602,o.value_coded,null)) as maternal_death_audited,
				max(if(o.concept_id=1573,o.value_coded,null)) as cadre,
				max(if(o.concept_id=120216,o.value_coded,null)) as delivery_complications,
				max(if(o.concept_id=1576,o.value_coded,null)) as coded_delivery_complications,
				max(if(o.concept_id=162093,o.value_text,null)) as other_delivery_complications,
				max(if(o.concept_id=159616,o.value_numeric,null)) as duration_of_labor,
				max(if(o.concept_id=1587,o.value_coded,null)) as baby_sex,
				max(if(o.concept_id=159917,o.value_coded,null)) as baby_condition,
				max(if(o.concept_id=1282 and o.value_coded = 84893,1,0)) as teo_given,
				max(if(o.concept_id=5916,o.value_numeric,null)) as birth_weight,
				max(if(o.concept_id=161543,o.value_coded,null)) as bf_within_one_hour,
				max(if(o.concept_id=164122,o.value_coded,null)) as birth_with_deformity,
				max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
				max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
				max(if(t.test_1_result is not null, t.expiry_, null)) as test_1_kit_expiry,
				max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
				max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
				max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
				max(if(t.test_2_result is not null, t.expiry_, null)) as test_2_kit_expiry,
				max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
				max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
				max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
				max(if(o.concept_id=161557,o.value_coded,null)) as partner_hiv_tested,
				max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
				max(if(o.concept_id=1109,o.value_coded,null)) as prophylaxis_given,
				max(if(o.concept_id = 1282 and o.value_coded = 160123,1,0)) as baby_azt_dispensed,
				max(if(o.concept_id = 1282 and o.value_coded = 80586,1,0)) as baby_nvp_dispensed,
				max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes

			from encounter e
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(162054,1789,5630,5599,162092,1856,162093,159603,159604,159605,162131,1572,1473,1379,1151,163454,1602,1573,162093,1576,120216,159616,1587,159917,1282,5916,161543,164122,159427,164848,161557,1436,1109,5576,159595,163784,159395)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('496c7cc3-0eea-4e84-a04c-2292949e2f7f')
				) f on f.form_id=e.form_id
				left join (
										select
											o.person_id,
											o.encounter_id,
											o.obs_group_id,
											max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
											max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
											max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
											max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
											max(if(o.concept_id=162502,(o.value_time),null)) as expiry_
										from obs o
											 inner join encounter e on e.encounter_id = o.encounter_id
											 inner join form f on f.form_id=e.form_id and f.uuid in ('496c7cc3-0eea-4e84-a04c-2292949e2f7f')
										 where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
										 group by e.encounter_id, o.obs_group_id
									 ) t on e.encounter_id = t.encounter_id
			group by e.encounter_id ;
		SELECT "Completed processing MCH Delivery visits", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_mchs_discharge-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_discharge$$
CREATE PROCEDURE sp_populate_etl_mch_discharge()
	BEGIN
		SELECT "Processing MCH Discharge ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mchs_discharge(
			patient_id,
			uuid,
			provider,
			visit_id,
			visit_,
			location_id,
			encounter_id,
			_created,
			counselled_on_feeding,
			baby_status,
			vitamin_A_dispensed,
			birth_notification_number,
			condition_of_mother,
			discharge_,
			referred_from,
			referred_to,
			clinical_notes
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.visit_id,
				e.encounter_time,
				e.location_id,
				e.encounter_id,
				e._created,
				max(if(o.concept_id=161651,o.value_coded,null)) as counselled_on_feeding,
				max(if(o.concept_id=159926,o.value_coded,null)) as baby_status,
				max(if(o.concept_id=161534,o.value_coded,null)) as vitamin_A_dispensed,
				max(if(o.concept_id=162051,o.value_text,null)) as birth_notification_number,
				max(if(o.concept_id=162093,o.value_text,null)) as condition_of_mother,
				max(if(o.concept_id=1641,o.value_time,null)) as discharge_,
				max(if(o.concept_id=160481,o.value_coded,null)) as referred_from,
				max(if(o.concept_id=163145,o.value_coded,null)) as referred_to,
				max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes
			from encounter e
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(161651,159926,161534,162051,162093,1641,160481,163145,159395)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('af273344-a5f9-11e8-98d0-529269fb1459')
				) f on f.form_id=e.form_id
			group by e.encounter_id ;
		SELECT "Completed processing MCH Discharge visits", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_mch_postnatal_visit-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_mch_postnatal_visit$$
CREATE PROCEDURE sp_populate_etl_mch_postnatal_visit()
	BEGIN
		SELECT "Processing MCH postnatal visits ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_mch_postnatal_visit(
			patient_id,
			uuid,
			visit_id,
			visit_,
			location_id,
			encounter_id,
			provider,
			pnc_register_no,
			pnc_visit_no,
			delivery_,
			mode_of_delivery,
			place_of_delivery,
			temperature,
			pulse_rate,
			systolic_bp,
			diastolic_bp,
			respiratory_rate,
			oxygen_saturation,
			weight,
			height,
			muac,
			hemoglobin,
			arv_status,
			general_condition,
			breast,
			cs_scar,
			gravid_uterus,
			episiotomy,
			lochia,
			pallor,
			pph,
			mother_hiv_status,
			condition_of_baby,
			baby_feeding_method,
			umblical_cord,
			baby_immunization_started,
			family_planning_counseling,
			uterus_examination,
			uterus_cervix_examination,
			vaginal_examination,
			parametrial_examination,
			external_genitalia_examination,
			ovarian_examination,
			pelvic_lymph_node_exam,
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
			partner_hiv_tested,
			partner_hiv_status,
			prophylaxis_given,
			baby_azt_dispensed,
			baby_nvp_dispensed,
			pnc_exercises,
			maternal_condition,
			iron_supplementation,
			fistula_screening,
			cacx_screening,
			cacx_screening_method,
			family_planning_status,
			family_planning_method,
			referred_from,
			referred_to,
			clinical_notes
		)
			select
				e.patient_id,
				e.uuid,
				e.visit_id,
				e.encounter_time,
				e.location_id,
				e.encounter_id,
				e.creator,
				max(if(o.concept_id=1646,o.value_text,null)) as pnc_register_no,
				max(if(o.concept_id=159893,o.value_numeric,null)) as pnc_visit_no,
				max(if(o.concept_id=5599,o.value_time,null)) as delivery_,
				max(if(o.concept_id=5630,o.value_coded,null)) as mode_of_delivery,
				max(if(o.concept_id=1572,o.value_coded,null)) as place_of_delivery,
				max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
				max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
				max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_bp,
				max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_bp,
				max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
				max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=5090,o.value_numeric,null)) as height,
				max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
				max(if(o.concept_id=21,o.value_numeric,null)) as hemoglobin,
				max(if(o.concept_id=1147,o.value_coded,null)) as arv_status,
				max(if(o.concept_id=1856,o.value_coded,null)) as general_condition,
				max(if(o.concept_id=159780,o.value_coded,null)) as breast,
				max(if(o.concept_id=162128,o.value_coded,null)) as cs_scar,
				max(if(o.concept_id=162110,o.value_coded,null)) as gravid_uterus,
				max(if(o.concept_id=159840,o.value_coded,null)) as episiotomy,
				max(if(o.concept_id=159844,o.value_coded,null)) as lochia,
				max(if(o.concept_id=5245,o.value_coded,null)) as pallor,
				max(if(o.concept_id=230,o.value_coded,null)) as pph,
				max(if(o.concept_id=1396,o.value_coded,null)) as mother_hiv_status,
				max(if(o.concept_id=162134,o.value_coded,null)) as condition_of_baby,
				max(if(o.concept_id=1151,o.value_coded,null)) as baby_feeding_method,
				max(if(o.concept_id=162121,o.value_coded,null)) as umblical_cord,
				max(if(o.concept_id=162127,o.value_coded,null)) as baby_immunization_started,
				max(if(o.concept_id=1382,o.value_coded,null)) as family_planning_counseling,
				max(if(o.concept_id=160967,o.value_text,null)) as uterus_examination,
				max(if(o.concept_id=160968,o.value_text,null)) as uterus_cervix_examination,
				max(if(o.concept_id=160969,o.value_text,null)) as vaginal_examination,
				max(if(o.concept_id=160970,o.value_text,null)) as parametrial_examination,
				max(if(o.concept_id=160971,o.value_text,null)) as external_genitalia_examination,
				max(if(o.concept_id=160975,o.value_text,null)) as ovarian_examination,
				max(if(o.concept_id=160972,o.value_text,null)) as pelvic_lymph_node_exam,
				max(if(t.test_1_result is not null, t.kit_name, null)) as test_1_kit_name,
				max(if(t.test_1_result is not null, t.lot_no, null)) as test_1_kit_lot_no,
				max(if(t.test_1_result is not null, t.expiry_, null)) as test_1_kit_expiry,
				max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
				max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
				max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
				max(if(t.test_2_result is not null, t.expiry_, null)) as test_2_kit_expiry,
				max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
				max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
				max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
				max(if(o.concept_id=161557,o.value_coded,null)) as partner_hiv_tested,
				max(if(o.concept_id=1436,o.value_coded,null)) as partner_hiv_status,
				max(if(o.concept_id=1109,o.value_coded,null)) as prophylaxis_given,
				max(if(o.concept_id=1282,o.value_coded,null)) as baby_azt_dispensed,
				max(if(o.concept_id=1282,o.value_coded,null)) as baby_nvp_dispensed,
				max(if(o.concept_id=161074,o.value_coded,null)) as pnc_exercises,
				max(if(o.concept_id=160085,o.value_coded,null)) as maternal_condition,
				max(if(o.concept_id=161004,o.value_coded,null)) as iron_supplementation,
				max(if(o.concept_id=159921,o.value_coded,null)) as fistula_screening,
				max(if(o.concept_id=164934,o.value_coded,null)) as cacx_screening,
				max(if(o.concept_id=163589,o.value_coded,null)) as cacx_screening_method,
				max(if(o.concept_id=160653,o.value_coded,null)) as family_planning_status,
				max(if(o.concept_id=374,o.value_coded,null)) as family_planning_method,
				max(if(o.concept_id=160481,o.value_coded,null)) as referred_from,
				max(if(o.concept_id=163145,o.value_coded,null)) as referred_to,
				max(if(o.concept_id=159395,o.value_text,null)) as clinical_notes


			from encounter e
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(1646,159893,5599,5630,1572,5088,5087,5085,5086,5242,5092,5089,5090,1343,21,1147,1856,159780,162128,162110,159840,159844,5245,230,1396,162134,1151,162121,162127,1382,160967,160968,160969,160970,160971,160975,160972,159427,164848,161557,1436,1109,5576,159595,163784,1282,161074,160085,161004,159921,164934,163589,160653,374,160481,163145,159395)
				inner join
				(
					select form_id, uuid,name from form where
						uuid in('72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7')
				) f on f.form_id= e.form_id
				left join (
										 select
											 o.person_id,
											 o.encounter_id,
											 o.obs_group_id,
											 max(if(o.concept_id=1040, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 163611 then "Invalid"  else "" end),null)) as test_1_result ,
											 max(if(o.concept_id=1326, (case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1175 then "N/A"  else "" end),null)) as test_2_result ,
											 max(if(o.concept_id=164962, (case o.value_coded when 164960 then "Determine" when 164961 then "First Response" else "" end),null)) as kit_name ,
											 max(if(o.concept_id=164964,trim(o.value_text),null)) as lot_no,
											 max(if(o.concept_id=162502,(o.value_time),null)) as expiry_
										 from obs o
											 inner join encounter e on e.encounter_id = o.encounter_id
											 inner join form f on f.form_id=e.form_id and f.uuid in ('72aa78e0-ee4b-47c3-9073-26f3b9ecc4a7')
										 where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
										 group by e.encounter_id, o.obs_group_id
									 ) t on e.encounter_id = t.encounter_id
			group by e.encounter_id;
		SELECT "Completed processing MCH postnatal visits ", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_hei_enrollment-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hei_enrolment$$
CREATE PROCEDURE sp_populate_etl_hei_enrolment()
	BEGIN
		SELECT "Processing HEI Enrollments", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_hei_enrollment(
			patient_id,
			uuid,
			provider,
			visit_id,
			visit_,
			location_id,
			encounter_id,
			child_exposed,
			-- hei_id_number,
			spd_number,
			birth_weight,
			gestation_at_birth,
			_first_seen,
			birth_notification_number,
			birth_certificate_number,
			need_for_special_care,
			reason_for_special_care,
			referral_source ,
			transfer_in,
			transfer_in_,
			facility_transferred_from,
			district_transferred_from,
			_first_enrolled_in_hei_care,
			-- arv_prophylaxis,
			mother_breastfeeding,
			-- mother_on_NVP_during_breastfeeding,
			TB_contact_history_in_household,
			-- infant_mother_link,
			mother_alive,
			mother_on_pmtct_drugs,
			mother_on_drug,
			mother_on_art_at_infant_enrollment,
			mother_drug_regimen,
			infant_prophylaxis,
			parent_ccc_number,
			mode_of_delivery,
			place_of_delivery,
			birth_length,
			birth_order,
			health_facility_name,
			_of_birth_notification,
			_of_birth_registration,
			birth_registration_place,
			permanent_registration_serial,
			mother_facility_registered,
			exit_,
      exit_reason,
      hiv_status_at_exit
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.visit_id,
				e.encounter_time,
				e.location_id,
				e.encounter_id,
				max(if(o.concept_id=5303,o.value_coded,null)) as child_exposed,
				-- max(if(o.concept_id=5087,o.value_numeric,null)) as hei_id_number,
				max(if(o.concept_id=162054,o.value_text,null)) as spd_number,
				max(if(o.concept_id=5916,o.value_numeric,null)) as birth_weight,
				max(if(o.concept_id=1409,o.value_numeric,null)) as gestation_at_birth,
				max(if(o.concept_id=162140,o.value_time,null)) as _first_seen,
				max(if(o.concept_id=162051,o.value_text,null)) as birth_notification_number,
				max(if(o.concept_id=162052,o.value_text,null)) as birth_certificate_number,
				max(if(o.concept_id=161630,o.value_coded,null)) as need_for_special_care,
				max(if(o.concept_id=161601,o.value_coded,null)) as reason_for_special_care,
				max(if(o.concept_id=160540,o.value_coded,null)) as referral_source,
				max(if(o.concept_id=160563,o.value_coded,null)) as transfer_in,
				max(if(o.concept_id=160534,o.value_time,null)) as transfer_in_,
				max(if(o.concept_id=160535,o.value_text,null)) as facility_transferred_from,
				max(if(o.concept_id=161551,o.value_text,null)) as district_transferred_from,
				max(if(o.concept_id=160555,o.value_time,null)) as _first_enrolled_in_hei_care,
				-- max(if(o.concept_id=1282,o.value_coded,null)) as arv_prophylaxis,
				max(if(o.concept_id=159941,o.value_coded,null)) as mother_breastfeeding,
				-- max(if(o.concept_id=1282,o.value_coded,null)) as mother_on_NVP_during_breastfeeding,
				max(if(o.concept_id=152460,o.value_coded,null)) as TB_contact_history_in_household,
				-- max(if(o.concept_id=162121,o.value_coded,null)) as infant_mother_link,
				max(if(o.concept_id=160429,o.value_coded,null)) as mother_alive,
				max(if(o.concept_id=1148,o.value_coded,null)) as mother_on_pmtct_drugs,
				max(if(o.concept_id=1086,o.value_coded,null)) as mother_on_drug,
				max(if(o.concept_id=162055,o.value_coded,null)) as mother_on_art_at_infant_enrollment,
				max(if(o.concept_id=1088,o.value_coded,null)) as mother_drug_regimen,
				max(if(o.concept_id=1282,o.value_coded,null)) as infant_prophylaxis,
				max(if(o.concept_id=162053,o.value_numeric,null)) as parent_ccc_number,
				max(if(o.concept_id=5630,o.value_coded,null)) as mode_of_delivery,
				max(if(o.concept_id=1572,o.value_coded,null)) as place_of_delivery,
				max(if(o.concept_id=1503,o.value_numeric,null)) as birth_length,
				max(if(o.concept_id=163460,o.value_numeric,null)) as birth_order,
				max(if(o.concept_id=162724,o.value_text,null)) as health_facility_name,
				max(if(o.concept_id=164130,o.value_time,null)) as _of_birth_notification,
				max(if(o.concept_id=164129,o.value_time,null)) as _of_birth_registration,
				max(if(o.concept_id=164140,o.value_text,null)) as birth_registration_place,
				max(if(o.concept_id=1646,o.value_text,null)) as permanent_registration_serial,
				max(if(o.concept_id=162724,o.value_text,null)) as mother_facility_registered,
			  max(if(o.concept_id=160753,o.value_time,null)) as exit_,
			  max(if(o.concept_id=161555,o.value_coded,null)) as exit_reason,
			  max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as hiv_status_at_exit
			from encounter e
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(5303,162054,5916,1409,162140,162051,162052,161630,161601,160540,160563,160534,160535,161551,160555,1282,159941,1282,152460,160429,1148,1086,162055,1088,1282,162053,5630,1572,161555,159427,1503,163460,162724,164130,164129,164140,1646,160753,161555,159427)

				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where
						uuid in('415f5136-ca4a-49a8-8db3-f994187c3af6','01894f88-dc73-42d4-97a3-0929118403fb')
				) et on et.encounter_type_id=e.encounter_type
			group by e.encounter_id ;
		SELECT "Completed processing HEI Enrollments", CONCAT("Time: ", NOW());
		END$$


-- ------------- populate etl_hei_follow_up_visit-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hei_follow_up$$
CREATE PROCEDURE sp_populate_etl_hei_follow_up()
	BEGIN
		SELECT "Processing HEI Followup visits", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_hei_follow_up_visit(
			patient_id,
			uuid,
			provider,
			visit_id,
			visit_,
			location_id,
			encounter_id,
			weight,
			height,
			primary_caregiver,
			infant_feeding,
			tb_assessment_outcome,
			social_smile_milestone,
			head_control_milestone,
			response_to_sound_milestone,
			hand_extension_milestone,
			sitting_milestone,
			walking_milestone,
			standing_milestone,
			talking_milestone,
			review_of_systems_developmental,
			dna_pcr_sample_,
			dna_pcr_contextual_status,
			dna_pcr_result,
			azt_given,
			nvp_given,
			ctx_given,
			-- dna_pcr_dbs_sample_code,
			-- dna_pcr_results_,
			-- first_antibody_sample_,
			first_antibody_result,
			-- first_antibody_dbs_sample_code,
			-- first_antibody_result_,
			-- final_antibody_sample_,
			final_antibody_result,
			-- final_antibody_dbs_sample_code,
			-- final_antibody_result_,
			tetracycline_ointment_given,
			pupil_examination,
			sight_examination,
			squint,
			deworming_drug,
			dosage,
			unit,
			comments,
			next_appointment_
		)
			select
				e.patient_id,
				e.uuid,
				e.creator,
				e.visit_id,
				e.encounter_time,
				e.location_id,
				e.encounter_id,
				max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
				max(if(o.concept_id=5090,o.value_numeric,null)) as height,
				max(if(o.concept_id=160640,o.value_coded,null)) as primary_caregiver,
				max(if(o.concept_id=1151,o.value_coded,null)) as infant_feeding,
				max(if(o.concept_id=1659,o.value_coded,null)) as tb_assessment_outcome,
				max(if(o.concept_id=162069 and o.value_coded=162056,o.value_coded,null)) as social_smile_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162057,o.value_coded,null)) as head_control_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162058,o.value_coded,null)) as response_to_sound_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162059,o.value_coded,null)) as hand_extension_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162061,o.value_coded,null)) as sitting_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162063,o.value_coded,null)) as walking_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162062,o.value_coded,null)) as standing_milestone,
				max(if(o.concept_id=162069 and o.value_coded=162060,o.value_coded,null)) as talking_milestone,
				max(if(o.concept_id=1189,o.value_coded,null)) as review_of_systems_developmental,
				max(if(o.concept_id=159951,o.value_time,null)) as dna_pcr_sample_,
				max(if(o.concept_id=162084,o.value_coded,null)) as dna_pcr_contextual_status,
				max(if(o.concept_id=1030,o.value_coded,null)) as dna_pcr_result,
				max(if(o.concept_id=966 and o.value_coded=86663,o.value_coded,null)) as azt_given,
				max(if(o.concept_id=966 and o.value_coded=80586,o.value_coded,null)) as nvp_given,
				max(if(o.concept_id=1109,o.value_coded,null)) as ctx_given,
				-- max(if(o.concept_id=162086,o.value_text,null)) as dna_pcr_dbs_sample_code,
				-- max(if(o.concept_id=160082,o.value_time,null)) as dna_pcr_results_,
				-- max(if(o.concept_id=159951,o.value_time,null)) as first_antibody_sample_,
				max(if(o.concept_id=1040,o.value_coded,null)) as first_antibody_result,
				-- max(if(o.concept_id=162086,o.value_text,null)) as first_antibody_dbs_sample_code,
				-- max(if(o.concept_id=160082,o.value_time,null)) as first_antibody_result_,
				-- max(if(o.concept_id=159951,o.value_time,null)) as final_antibody_sample_,
				max(if(o.concept_id=1326,o.value_coded,null)) as final_antibody_result,
				-- max(if(o.concept_id=162086,o.value_text,null)) as final_antibody_dbs_sample_code,
				-- max(if(o.concept_id=160082,o.value_time,null)) as final_antibody_result_,
				max(if(o.concept_id=162077,o.value_coded,null)) as tetracycline_ointment_given,
				max(if(o.concept_id=162064,o.value_coded,null)) as pupil_examination,
				max(if(o.concept_id=162067,o.value_coded,null)) as sight_examination,
				max(if(o.concept_id=162066,o.value_coded,null)) as squint,
				max(if(o.concept_id=1282,o.value_coded,null)) as deworming_drug,
				max(if(o.concept_id=1443,o.value_numeric,null)) as dosage,
				max(if(o.concept_id=1621,o.value_text,null)) as unit,
				max(if(o.concept_id=159395,o.value_text,null)) as comments,
				max(if(o.concept_id=5096,o.value_time,null)) as next_appointment_
			from encounter e
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
														and o.concept_id in(844,5089,5090,160640,1151,1659,5096,162069,162069,162069,162069,162069,162069,162069,162069,1189,159951,966,1109,162084,1030,162086,160082,159951,1040,162086,160082,159951,1326,162086,160082,162077,162064,162067,162066,1282,1443,1621,159395,5096)
				inner join
				(
					select encounter_type_id, uuid, name from encounter_type where
						uuid in('bcc6da85-72f2-4291-b206-789b8186a021','c6d09e05-1f25-4164-8860-9f32c5a02df0')
				) et on et.encounter_type_id=e.encounter_type
			group by e.encounter_id ;
		SELECT "Completed processing HEI Followup visits", CONCAT("Time: ", NOW());
		END$$

-- ------------- populate etl_immunization   --------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hei_immunization$$
CREATE PROCEDURE sp_populate_etl_hei_immunization()
	BEGIN
		SELECT "Processing hei_immunization data ", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_hei_immunization(
      patient_id,
      visit_,
      created_by,
      _created,
      encounter_id,
      BCG,
      OPV_birth,
      OPV_1,
      OPV_2,
      OPV_3,
      IPV,
      DPT_Hep_B_Hib_1,
      DPT_Hep_B_Hib_2,
      DPT_Hep_B_Hib_3,
      PCV_10_1,
      PCV_10_2,
      PCV_10_3,
      ROTA_1,
      ROTA_2,
      Measles_rubella_1,
      Measles_rubella_2,
      Yellow_fever,
			Measles_6_months,
			VitaminA_6_months,
			VitaminA_1_yr,
			VitaminA_1_and_half_yr,
			VitaminA_2_yr ,
			VitaminA_2_to_5_yr,
			fully_immunized
    )
      select
        patient_id,
        visit_,
        y.creator,
        y._created,
        y.encounter_id,
        max(if(vaccine="BCG", _given, "")) as BCG,
        max(if(vaccine="OPV" and sequence=0, _given, "")) as OPV_birth,
        max(if(vaccine="OPV" and sequence=1, _given, "")) as OPV_1,
        max(if(vaccine="OPV" and sequence=2, _given, "")) as OPV_2,
        max(if(vaccine="OPV" and sequence=3, _given, "")) as OPV_3,
        max(if(vaccine="IPV", _given, ""))  as IPV,
        max(if(vaccine="DPT" and sequence=1, _given, "")) as DPT_Hep_B_Hib_1,
        max(if(vaccine="DPT" and sequence=2, _given, "")) as DPT_Hep_B_Hib_2,
        max(if(vaccine="DPT" and sequence=3, _given, "")) as DPT_Hep_B_Hib_3,
        max(if(vaccine="PCV" and sequence=1, _given, "")) as PCV_10_1,
        max(if(vaccine="PCV" and sequence=2, _given, "")) as PCV_10_2,
        max(if(vaccine="PCV" and sequence=3, _given, "")) as PCV_10_3,
        max(if(vaccine="ROTA" and sequence=1, _given, "")) as ROTA_1,
        max(if(vaccine="ROTA" and sequence=2, _given, "")) as ROTA_2,
        max(if(vaccine="measles_rubella" and sequence=1, _given, "")) as Measles_rubella_1,
        max(if(vaccine="measles_rubella" and sequence=2, _given, "")) as Measles_rubella_2,
        max(if(vaccine="yellow_fever", _given, "")) as Yellow_fever,
        max(if(vaccine="measles", _given, "")) as Measles_6_months,
        max(if(vaccine="Vitamin A" and sequence=1, _given, "")) as VitaminA_6_months,
        max(if(vaccine="Vitamin A" and sequence=2, _given, "")) as VitaminA_1_yr,
        max(if(vaccine="Vitamin A" and sequence=3, _given, "")) as VitaminA_1_and_half_yr,
        max(if(vaccine="Vitamin A" and sequence=4, _given, "")) as VitaminA_2_yr,
        max(if(vaccine="Vitamin A" and sequence=5, _given, "")) as VitaminA_2_to_5_yr,
				max((o.value_time)) as fully_immunized
      from (
						 (select
								person_id as patient_id,
								(encounter_time) as visit_,
								creator,
								(_created) as _created,
								encounter_id,
								name as encounter_type,
								max(if(concept_id=1282 , "Vitamin A", "")) as vaccine,
								max(if(concept_id=1418, value_numeric, "")) as sequence,
								max(if(concept_id=1282 , (obs_time), "")) as _given,
								obs_group_id
							from (
										 select o.person_id, e.encounter_time, e.creator, e._created, o.concept_id, o.value_coded, o.value_numeric, (o.value_time) _given, o.obs_group_id, o.encounter_id, et.uuid, et.name, o.obs_time
										 from obs o
											 inner join encounter e on e.encounter_id=o.encounter_id
											 inner join
											 (
												 select encounter_type_id, uuid, name from encounter_type where
													 uuid = '82169b8d-c945-4c41-be62-433dfd9d6c86'
											 ) et on et.encounter_type_id=e.encounter_type
										 where concept_id in(1282,1418) and o.voided=0
									 ) t
							group by obs_group_id
							having vaccine != ""
						 )
						 union
						 (
							 select
								 person_id as patient_id,
								 (encounter_time) as visit_,
								 creator,
								 (_created) as _created,
								 encounter_id,
								 name as encounter_type,
								 max(if(concept_id=984 , (case when value_coded=886 then "BCG" when value_coded=783 then "OPV" when value_coded=1422 then "IPV"
																					when value_coded=781 then "DPT" when value_coded=162342 then "PCV" when value_coded=83531 then "ROTA"
																					when value_coded=162586 then "measles_rubella"  when value_coded=5864 then "yellow_fever" when value_coded=36 then "measles" when value_coded=84879 then "TETANUS TOXOID"  end), "")) as vaccine,
								 max(if(concept_id=1418, value_numeric, "")) as sequence,
								 max(if(concept_id=1410, _given, "")) as _given,
								 obs_group_id
							 from (
											select o.person_id, e.encounter_time, e.creator, e._created, o.concept_id, o.value_coded, o.value_numeric, (o.value_time) _given, o.obs_group_id, o.encounter_id, et.uuid, et.name
											from obs o
												inner join encounter e on e.encounter_id=o.encounter_id
												inner join
												(
													select encounter_type_id, uuid, name from encounter_type where
														uuid = '82169b8d-c945-4c41-be62-433dfd9d6c86'
												) et on et.encounter_type_id=e.encounter_type
											where concept_id in(984,1418,1410) and o.voided=0
										) t
							 group by obs_group_id
							 having vaccine != ""
						 )
           ) y
				left join obs o on y.encounter_id = o.encounter_id and o.concept_id=162585 and o.voided=0

      group by patient_id;

	SELECT "Completed processing hei_immunization data ", CONCAT("Time: ", NOW());
	END$$

		-- ------------- populate etl_tb_enrollment-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_tb_enrollment$$
CREATE PROCEDURE sp_populate_etl_tb_enrollment()
BEGIN
SELECT "Processing TB Enrollments ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_tb_enrollment(
patient_id,
uuid,
provider,
visit_id,
visit_,
location_id,
encounter_id,
_treatment_started,
district,
-- district_registration_number,
referred_by,
referral_,
_transferred_in,
facility_transferred_from,
district_transferred_from,
_first_enrolled_in_tb_care,
weight,
height,
treatment_supporter,
relation_to_patient,
treatment_supporter_address,
treatment_supporter_phone_contact,
disease_classification,
patient_classification,
pulmonary_smear_result,
has_extra_pulmonary_pleurial_effusion,
has_extra_pulmonary_milliary,
has_extra_pulmonary_lymph_node,
has_extra_pulmonary_menengitis,
has_extra_pulmonary_skeleton,
has_extra_pulmonary_abdominal
-- has_extra_pulmonary_other,
-- treatment_outcome,
-- treatment_outcome_
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_time,
e.location_id,
e.encounter_id,
max(if(o.concept_id=1113,o.value_time,null)) as _treatment_started,
max(if(o.concept_id=161564,trim(o.value_text),null)) as district,
-- max(if(o.concept_id=5085,o.value_numeric,null)) as district_registration_number,
max(if(o.concept_id=160540,o.value_coded,null)) as referred_by,
max(if(o.concept_id=161561,o.value_time,null)) as referral_,
max(if(o.concept_id=160534,o.value_time,null)) as _transferred_in,
max(if(o.concept_id=160535,left(trim(o.value_text),100),null)) as facility_transferred_from,
max(if(o.concept_id=161551,left(trim(o.value_text),100),null)) as district_transferred_from,
max(if(o.concept_id=161552,o.value_time,null)) as _first_enrolled_in_tb_care,
max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
max(if(o.concept_id=5090,o.value_numeric,null)) as height,
max(if(o.concept_id=160638,left(trim(o.value_text),100),null)) as treatment_supporter,
max(if(o.concept_id=160640,o.value_coded,null)) as relation_to_patient,
max(if(o.concept_id=160641,left(trim(o.value_text),100),null)) as treatment_supporter_address,
max(if(o.concept_id=160642,left(trim(o.value_text),100),null)) as treatment_supporter_phone_contact,
max(if(o.concept_id=160040,o.value_coded,null)) as disease_classification,
max(if(o.concept_id=159871,o.value_coded,null)) as patient_classification,
max(if(o.concept_id=159982,o.value_coded,null)) as pulmonary_smear_result,
max(if(o.concept_id=161356 and o.value_coded=130059,o.value_coded,null)) as has_extra_pulmonary_pleurial_effusion,
max(if(o.concept_id=161356 and o.value_coded=115753,o.value_coded,null)) as has_extra_pulmonary_milliary,
max(if(o.concept_id=161356 and o.value_coded=111953,o.value_coded,null)) as has_extra_pulmonary_lymph_node,
max(if(o.concept_id=161356 and o.value_coded=111967,o.value_coded,null)) as has_extra_pulmonary_menengitis,
max(if(o.concept_id=161356 and o.value_coded=112116,o.value_coded,null)) as has_extra_pulmonary_skeleton,
max(if(o.concept_id=161356 and o.value_coded=1350,o.value_coded,null)) as has_extra_pulmonary_abdominal
-- max(if(o.concept_id=161356,o.value_coded,null)) as has_extra_pulmonary_other
-- max(if(o.concept_id=159786,o.value_coded,null)) as treatment_outcome,
-- max(if(o.concept_id=159787,o.value_coded,null)) as treatment_outcome_

from encounter e
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
and o.concept_id in(160540,161561,160534,160535,161551,161552,5089,5090,160638,160640,160641,160642,160040,159871,159982,161356)
inner join
(
	select encounter_type_id, uuid, name from encounter_type where
	uuid in('9d8498a4-372d-4dc4-a809-513a2434621e')
) et on et.encounter_type_id=e.encounter_type
group by e.encounter_id;
SELECT "Completed processing TB Enrollments ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_tb_follow_up_visit-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_tb_follow_up_visit$$
CREATE PROCEDURE sp_populate_etl_tb_follow_up_visit()
BEGIN
SELECT "Processing TB Followup visits ", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_tb_follow_up_visit(
patient_id,
uuid,
provider,
visit_id ,
visit_ ,
location_id,
encounter_id,
spatum_test,
spatum_result,
result_serial_number,
quantity ,
_test_done,
bacterial_colonie_growth,
number_of_colonies,
resistant_s,
resistant_r,
resistant_inh,
resistant_e,
sensitive_s,
sensitive_r,
sensitive_inh,
sensitive_e,
test_,
hiv_status,
next_appointment_
)
select
e.patient_id,
e.uuid,
e.creator,
e.visit_id,
e.encounter_time,
e.location_id,
e.encounter_id,
max(if(o.concept_id=159961,o.value_coded,null)) as spatum_test,
max(if(o.concept_id=307,o.value_coded,null)) as spatum_result,
max(if(o.concept_id=159968,o.value_numeric,null)) as result_serial_number,
max(if(o.concept_id=160023,o.value_numeric,null)) as quantity,
max(if(o.concept_id=159964,o.value_time,null)) as _test_done,
max(if(o.concept_id=159982,o.value_coded,null)) as bacterial_colonie_growth,
max(if(o.concept_id=159952,o.value_numeric,null)) as number_of_colonies,
max(if(o.concept_id=159956 and o.value_coded=84360,o.value_coded,null)) as resistant_s,
max(if(o.concept_id=159956 and o.value_coded=767,o.value_coded,null)) as resistant_r,
max(if(o.concept_id=159956 and o.value_coded=78280,o.value_coded,null)) as resistant_inh,
max(if(o.concept_id=159956 and o.value_coded=75948,o.value_coded,null)) as resistant_e,
max(if(o.concept_id=159958 and o.value_coded=84360,o.value_coded,null)) as sensitive_s,
max(if(o.concept_id=159958 and o.value_coded=767,o.value_coded,null)) as sensitive_r,
max(if(o.concept_id=159958 and o.value_coded=78280,o.value_coded,null)) as sensitive_inh,
max(if(o.concept_id=159958 and o.value_coded=75948,o.value_coded,null)) as sensitive_e,
max(if(o.concept_id=159964,o.value_time,null)) as test_,
max(if(o.concept_id=1169,o.value_coded,null)) as hiv_status,
max(if(o.concept_id=5096,o.value_time,null)) as next_appointment_
from encounter e
inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
and o.concept_id in(159961,307,159968,160023,159964,159982,159952,159956,159958,159964,1169,5096)
inner join
(
	select encounter_type_id, uuid, name from encounter_type where
	uuid in('fbf0bfce-e9f4-45bb-935a-59195d8a0e35')
) et on et.encounter_type_id=e.encounter_type
group by e.encounter_id;
SELECT "Completed processing TB Followup visits ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_tb_screening-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_tb_screening$$
CREATE PROCEDURE sp_populate_etl_tb_screening()
BEGIN
SELECT "Processing TB Screening data ", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_tb_screening(
patient_id,
uuid,
provider,
visit_id,
visit_,
encounter_id,
location_id,
resulting_tb_status ,
tb_treatment_start_,
notes
)
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_time, e.encounter_id, e.location_id,
max(case o.concept_id when 1659 then o.value_coded else null end) as resulting_tb_status,
max(case o.concept_id when 1113 then (o.value_time)  else NULL end) as tb_treatment_start_,
"" as notes -- max(case o.concept_id when 160632 then value_text else "" end) as notes
from encounter e
inner join form f on f.form_id=e.form_id and f.uuid in ("22c68f86-bbf0-49ba-b2d1-23fa7ccf0259", "59ed8e62-7f1f-40ae-a2e3-eabe350277ce")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (1659, 1113, 160632) and o.voided=0
where e.voided=0
group by e.encounter_id;

SELECT "Completed processing TB Screening data ", CONCAT("Time: ", NOW());
END$$

-- ------------------------------------------- drug event ---------------------------

DROP PROCEDURE IF EXISTS sp_drug_event$$
CREATE PROCEDURE sp_drug_event()
BEGIN
SELECT "Processing Drug Event Data", CONCAT("Time: ", NOW());
	INSERT INTO kenyaemr_etl.etl_drug_event(
		uuid,
		patient_id,
		_started,
		visit_,
		provider,
		encounter_id,
		program,
		regimen,
		regimen_name,
		regimen_line,
		discontinued,
		regimen_discontinued,
		_discontinued,
		reason_discontinued,
		reason_discontinued_other
	)
		select
			e.uuid,
			e.patient_id,
			e.encounter_time,
			e.encounter_time,
			e.creator,
			e.encounter_id,
			max(if(o.concept_id=1255,'HIV',if(o.concept_id=1268, 'TB', null))) as program,
			max(if(o.concept_id=1193,(
				case o.value_coded
				when 162565 then "3TC/NVP/TDF"
				when 164505 then "TDF/3TC/EFV"
				when 1652 then "AZT/3TC/NVP"
				when 160124 then "AZT/3TC/EFV"
				when 792 then "D4T/3TC/NVP"
				when 160104 then "D4T/3TC/EFV"
				when 164971 then "TDF/3TC/AZT"
				when 164968 then "AZT/3TC/DTG"
				when 164969 then "TDF/3TC/DTG"
				when 164970 then "ABC/3TC/DTG"
				when 162561 then "AZT/3TC/LPV/r"
				when 164511 then "AZT/3TC/ATV/r"
				when 162201 then "TDF/3TC/LPV/r"
				when 164512 then "TDF/3TC/ATV/r"
				when 162560 then "D4T/3TC/LPV/r"
				when 164972 then "AZT/TDF/3TC/LPV/r"
				when 164973 then "ETR/RAL/DRV/RTV"
				when 164974 then "ETR/TDF/3TC/LPV/r"
				when 162200 then "ABC/3TC/LPV/r"
				when 162199 then "ABC/3TC/NVP"
				when 162563 then "ABC/3TC/EFV"
				when 817 then "AZT/3TC/ABC"
				when 164975 then "D4T/3TC/ABC"
				when 162562 then "TDF/ABC/LPV/r"
				when 162559 then "ABC/DDI/LPV/r"
				when 164976 then "ABC/TDF/3TC/LPV/r"
				when 1675 then "RHZE"
				when 768 then "RHZ"
				when 1674 then "SRHZE"
				when 164978 then "RfbHZE"
				when 164979 then "RfbHZ"
				when 164980 then "SRfbHZE"
				when 84360 then "S (1 gm vial)"
				when 75948 then "E"
				when 1194 then "RH"
				when 159851 then "RHE"
				when 1108 then "EH"
				else ""
				end ),null)) as regimen,
			max(if(o.concept_id=1193,(
				case o.value_coded
				when 162565 then "3TC+NVP+TDF"
				when 164505 then "TDF+3TC+EFV"
				when 1652 then "AZT+3TC+NVP"
				when 160124 then "AZT+3TC+EFV"
				when 792 then "D4T+3TC+NVP"
				when 160104 then "D4T+3TC+EFV"
				when 164971 then "TDF+3TC+AZT"
				when 164968 then "AZT+3TC+DTG"
				when 164969 then "TDF+3TC+DTG"
				when 164970 then "ABC+3TC+DTG"
				when 162561 then "AZT+3TC+LPV/r"
				when 164511 then "AZT+3TC+ATV/r"
				when 162201 then "TDF+3TC+LPV/r"
				when 164512 then "TDF+3TC+ATV/r"
				when 162560 then "D4T+3TC+LPV/r"
				when 164972 then "AZT+TDF+3TC+LPV/r"
				when 164973 then "ETR+RAL+DRV+RTV"
				when 164974 then "ETR+TDF+3TC+LPV/r"
				when 162200 then "ABC+3TC+LPV/r"
				when 162199 then "ABC+3TC+NVP"
				when 162563 then "ABC+3TC+EFV"
				when 817 then "AZT+3TC+ABC"
				when 164975 then "D4T+3TC+ABC"
				when 162562 then "TDF+ABC+LPV/r"
				when 162559 then "ABC+DDI+LPV/r"
				when 164976 then "ABC+TDF+3TC+LPV/r"
				when 1675 then "RHZE"
				when 768 then "RHZ"
				when 1674 then "SRHZE"
				when 164978 then "RfbHZE"
				when 164979 then "RfbHZ"
				when 164980 then "SRfbHZE"
				when 84360 then "S (1 gm vial)"
				when 75948 then "E"
				when 1194 then "RH"
				when 159851 then "RHE"
				when 1108 then "EH"
				else ""
				end ),null)) as regimen_name,
			max(if(o.concept_id=1193,(
				case o.value_coded
				-- adult first line
				when 162565 then "Adult first line"
				when 164505 then "Adult first line"
				when 1652 then "Adult first line"
				when 160124 then "Adult first line"
				when 792 then "Adult first line"
				when 160104 then "Adult first line"
				when 164971 then "Adult first line"
				when 164968 then "Adult first line"
				when 164969 then "Adult first line"
				when 164970 then "Adult first line"
				-- adult second line
				when 162561 then "Adult second line"
				when 164511 then "Adult second line"
				when 162201 then "Adult second line"
				when 164512 then "Adult second line"
				when 162560 then "Adult second line"
				when 164972 then "Adult second line"
				when 164973 then "Adult second line"
				when 164974 then "Adult second line"
				-- child 1st line
				when 162200 then "Child first line"
				when 162199 then "Child first line"
				when 162563 then "Child first line"
				when 817 then "Child first line"
				when 164975 then "Child first line"
				when 162562 then "Child first line"
				when 162559 then "Child first line"
				when 164976 then "Child first line"
				-- tb
				when 1675 then "Adult intensive"
				when 768 then "Adult intensive"
				when 1674 then "Adult intensive"
				when 164978 then "Adult intensive"
				when 164979 then "Adult intensive"
				when 164980 then "Adult intensive"
				when 84360 then "Adult intensive"
				-- child intensive
				when 75948 then "Child intensive"
				when 1194 then "Child intensive"
				-- adult continuation
				when 159851 then "Adult continuation"
				when 1108 then "Adult continuation"
				else ""
				end ),null)) as regimen_line,
			max(if(o.concept_id=1191,(case o.value_time when NULL then 0 else 1 end),null)) as discontinued,
			null as regimen_discontinued,
			max(if(o.concept_id=1191,o.value_time,null)) as _discontinued,
			max(if(o.concept_id=1252,o.value_coded,null)) as reason_discontinued,
			max(if(o.concept_id=5622,o.value_text,null)) as reason_discontinued_other

		from encounter e
			inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
													and o.concept_id in(1193,1252,5622,1191,1255,1268)
			inner join
			(
				select encounter_type, uuid,name from form where
					uuid in('da687480-e197-11e8-9f32-f2801f1b9fd1') -- regimen editor form
			) f on f.encounter_type=e.encounter_type
		group by e.encounter_id
		order by e.patient_id, e.encounter_time;

SELECT "Completed processing Drug Event Data", CONCAT("Time: ", NOW());
END$$



-- ------------------------------------ populate hts test table ----------------------------------------


DROP PROCEDURE IF EXISTS sp_populate_hts_test$$
CREATE PROCEDURE sp_populate_hts_test()
BEGIN
SELECT "Processing hts tests";
INSERT INTO kenyaemr_etl.etl_hts_test (
patient_id,
visit_id,
encounter_id,
encounter_uuid,
encounter_location,
creator,
_created,
visit_,
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
e._created,
e.encounter_time as visit_,
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
max(if(t.test_1_result is not null, t.expiry_, null)) as test_1_kit_expiry,
max(if(t.test_1_result is not null, t.test_1_result, null)) as test_1_result,
max(if(t.test_2_result is not null, t.kit_name, null)) as test_2_kit_name,
max(if(t.test_2_result is not null, t.lot_no, null)) as test_2_kit_lot_no,
max(if(t.test_2_result is not null, t.expiry_, null)) as test_2_kit_expiry,
max(if(t.test_2_result is not null, t.test_2_result, null)) as test_2_result,
max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" when 1138 then "Inconclusive" else "" end),null)) as final_test_result,
max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_given_result,
max(if(o.concept_id=6096,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as couple_discordant,
max(if(o.concept_id=1659,(case o.value_coded when 1660 then "No TB signs" when 142177 then "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "Not done" when 1111 then "On TB Treatment"  else "" end),null)) as tb_screening,
max(if(o.concept_id=164952,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as patient_had_hiv_self_test,
max(if(o.concept_id=163042,trim(o.value_text),null)) as remarks,
e.voided
from encounter e
inner join form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (162084, 164930, 160581, 164401, 164951, 162558, 1710, 164959, 164956,
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
               max(if(o.concept_id=162502,(o.value_time),null)) as expiry_
             from obs o
             inner join encounter e on e.encounter_id = o.encounter_id
             inner join form f on f.form_id=e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
             where o.concept_id in (1040, 1326, 164962, 164964, 162502) and o.voided=0
             group by e.encounter_id, o.obs_group_id
           ) t on e.encounter_id = t.encounter_id
group by e.encounter_id;
SELECT "Completed processing hts tests";
END$$


-- ------------------------------------ POPULATE HTS LINKAGES AND REFERRALS -------------------------------

DROP PROCEDURE IF EXISTS sp_populate_hts_linkage_and_referral$$
CREATE PROCEDURE sp_populate_hts_linkage_and_referral()
BEGIN
SELECT "Processing hts linkages, referrals and tracing";
INSERT INTO kenyaemr_etl.etl_hts_referral_and_linkage (
  patient_id,
  visit_id,
  encounter_id,
  encounter_uuid,
  encounter_location,
  creator,
  _created,
  visit_,
  tracing_type,
  tracing_status,
  facility_linked_to,
	enrollment_,
	art_start_,
  ccc_number,
  provider_handed_to,
  voided
)
  select
    e.patient_id,
    e.visit_id,
    e.encounter_id,
    e.uuid,
    e.location_id,
    e.creator,
    e._created,
    e.encounter_time as visit_,
    max(if(o.concept_id=164966,(case o.value_coded when 1650 then "Phone" when 164965 then "Physical" else "" end),null)) as tracing_type ,
    max(if(o.concept_id=159811,(case o.value_coded when 1065 then "Contacted and linked" when 1066 then "Contacted but not linked" else "" end),null)) as tracing_status,
    max(if(o.concept_id=162724,trim(o.value_text),null)) as facility_linked_to,
		max(if(o.concept_id=160555,o.value_time,null)) as enrollment_,
		max(if(o.concept_id=159599,o.value_time,null)) as art_start_,
    max(if(o.concept_id=162053,o.value_numeric,null)) as ccc_number,
    max(if(o.concept_id=1473,trim(o.value_text),null)) as provider_handed_to,
    e.voided
  from encounter e
  inner join form f on f.form_id = e.form_id and f.uuid = "050a7f12-5c52-4cad-8834-863695af335d"
  left outer join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164966, 159811, 162724, 160555, 159599, 162053, 1473) and o.voided=0
  group by e.encounter_id;

-- fetch locally enrolled clients who had went through HTS
/*
  INSERT INTO kenyaemr_etl.etl_hts_referral_and_linkage (
  patient_id,
  visit_id,
  encounter_id,
  encounter_uuid,
  encounter_location,
  creator,
  _created,
  visit_,
  tracing_status,
  facility_linked_to,
  ccc_number,
  voided
)
select
    e.patient_id,
    e.visit_id,
    e.encounter_id,
    e.uuid,
    e.location_id,
    e.creator,
    e._created,
    e.encounter_time as visit_,
    "Enrolled" as contact_status,
    (select name from location
        where location_id in (select property_value
        from global_property
        where property='kenyaemr.defaultLocation'))  as facility_linked_to,
    pi.identifier as ccc_number,
    e.voided
 from encounter e
 inner join encounter_type et on e.encounter_type = et.encounter_type_id and et.uuid = "de78a6be-bfc5-4634-adc3-5f1a280455cc"
 inner join form f on f.form_id = e.form_id and f.uuid in ("402dc5d7-46da-42d4-b2be-f43ea4ad87b0","b08471f6-0892-4bf7-ab2b-bf79797b8ea4")
 left outer join patient_identifier pi on pi.patient_id = e.patient_id
 left join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id and pit.uuid = '05ee9cf4-7242-4a17-b4d4-00f707265c8a'
;*/

END$$

-- ----------------------------------- UP DASHBOARD TABLE ---------------------


DROP PROCEDURE IF EXISTS sp_up_dashboard_table$$
CREATE PROCEDURE sp_up_dashboard_table()
BEGIN

DECLARE start ;
DECLARE end ;
DECLARE reportingPeriod VARCHAR(20);

SET start = _FORMAT(NOW() - INTERVAL 1 MONTH, '%Y-%m-01');
SET end = _FORMAT(LAST_DAY(NOW() - INTERVAL 1 MONTH), '%Y-%m-%d');
SET reportingPeriod = _FORMAT(NOW() - INTERVAL 1 MONTH, '%Y-%M');

-- CURRENT IN CARE
DROP TABLE IF EXISTS kenyaemr_etl.etl_current_in_care;

CREATE TABLE kenyaemr_etl.etl_current_in_care AS
select fup.visit_,fup.patient_id,p.dob,p.Gender, min(e.visit_) as enroll_,
max(fup.visit_) as latest_vis_,
mid(max(concat(fup.visit_,fup.next_appointment_)),11) as latest_tca,
p.unique_patient_no,
max(d.visit_) as _discontinued,
d.patient_id as disc_patient,
de.patient_id as started_on_drugs
from kenyaemr_etl.etl_patient_hiv_followup fup
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=fup.patient_id
join kenyaemr_etl.etl_hiv_enrollment e on fup.patient_id=e.patient_id
left outer join kenyaemr_etl.etl_drug_event de on e.patient_id = de.patient_id and (_started) <= end
left outer JOIN
(select patient_id, visit_ from kenyaemr_etl.etl_patient_program_discontinuation
where (visit_) <= end and program_name='HIV'
group by patient_id
) d on d.patient_id = fup.patient_id
where fup.visit_ <= end
group by patient_id
having (
((latest_tca) > end and ((latest_tca) > (_discontinued) or disc_patient is null )) or
((((latest_tca) between start and end) and (((latest_vis_) >= (latest_tca)) or (latest_tca) > cur())) and ((latest_tca) > (_discontinued) or disc_patient is null )) )
;

-- ADD INDICES
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(enroll_);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(latest_vis_);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(latest_tca);
ALTER TABLE kenyaemr_etl.etl_current_in_care ADD INDEX(started_on_drugs);


DROP TABLE IF EXISTS kenyaemr_etl.etl_last_month_newly_enrolled_in_care;
CREATE TABLE kenyaemr_etl.etl_last_month_newly_enrolled_in_care (
patient_id  not null
);

INSERT INTO kenyaemr_etl.etl_last_month_newly_enrolled_in_care
select distinct e.patient_id
from kenyaemr_etl.etl_hiv_enrollment e
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=e.patient_id
where  e.entry_point <> 160563  and transfer_in_ is null
and (e.visit_) between start and end and (e.patient_type not in (160563, 164931, 159833) or e.patient_type is null or e.patient_type='');


DROP TABLE IF EXISTS kenyaemr_etl.etl_last_month_newly_on_art;
CREATE TABLE kenyaemr_etl.etl_last_month_newly_on_art (
patient_id  not null
);

INSERT INTO kenyaemr_etl.etl_last_month_newly_on_art
select distinct net.patient_id
from (
select e.patient_id,e._started,
e.gender,
e.dob,
d.visit_ as dis_,
if(d.visit_ is not null, 1, 0) as TOut,
e.regimen, e.regimen_line, e.alternative_regimen,
mid(max(concat(fup.visit_,fup.next_appointment_)),11) as latest_tca,
max(if(enr._started_art_at_transferring_facility is not null and enr.facility_transferred_from is not null, 1, 0)) as TI_on_art,
max(if(enr.transfer_in_ is not null, 1, 0)) as TIn,
max(fup.visit_) as latest_vis_
from (select e.patient_id,p.dob,p.Gender,min(e._started) as _started,
mid(min(concat(e._started,e.regimen_name)),11) as regimen,
mid(min(concat(e._started,e.regimen_line)),11) as regimen_line,
max(if(discontinued,1,0))as alternative_regimen
from kenyaemr_etl.etl_drug_event e
join kenyaemr_etl.etl_patient_demographics p on p.patient_id=e.patient_id
group by e.patient_id) e
left outer join kenyaemr_etl.etl_patient_program_discontinuation d on d.patient_id=e.patient_id
left outer join kenyaemr_etl.etl_hiv_enrollment enr on enr.patient_id=e.patient_id
left outer join kenyaemr_etl.etl_patient_hiv_followup fup on fup.patient_id=e.patient_id
where  (e._started) between start and end
group by e.patient_id
having TI_on_art=0
)net;

-- populate people booked today
TRUNCATE TABLE kenyaemr_etl.etl_patients_booked_today;
ALTER TABLE kenyaemr_etl.etl_patients_booked_today AUTO_INCREMENT = 1;

INSERT INTO kenyaemr_etl.etl_patients_booked_today(patient_id, last_visit_)
SELECT patient_id, max(visit_)
FROM kenyaemr_etl.etl_patient_hiv_followup
WHERE (next_appointment_) = CUR()
GROUP BY patient_id;

SELECT "Completed processing dashboard indicators", CONCAT("Time: ", NOW());
END$$


-- ------------- populate etl_ipt_screening-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_screening$$
CREATE PROCEDURE sp_populate_etl_ipt_screening()
BEGIN
SELECT "Processing IPT screening forms", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_ipt_screening(
patient_id,
uuid,
provider,
visit_id,
visit_,
encounter_id,
location_id,
ipt_started
)
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_time, e.encounter_id, e.location_id,
max(o.value_coded) as ipt_started
from encounter e
inner join form f on f.form_id=e.form_id and f.uuid in ("22c68f86-bbf0-49ba-b2d1-23fa7ccf0259", "59ed8e62-7f1f-40ae-a2e3-eabe350277ce")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id=1265 and o.voided=0
where e.voided=0
group by e.encounter_id;

SELECT "Completed processing IPT screening forms", CONCAT("Time: ", NOW());
END$$



-- ------------- populate etl_ipt_followup-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ipt_follow_up$$
CREATE PROCEDURE sp_populate_etl_ipt_follow_up()
BEGIN
SELECT "Processing IPT followup forms", CONCAT("Time: ", NOW());
insert into kenyaemr_etl.etl_ipt_follow_up(
patient_id,
uuid,
provider,
visit_id,
visit_,
encounter_id,
location_id,
ipt_due_,
_collected_ipt,
hepatotoxity,
peripheral_neuropathy,
rash,
adherence,
outcome,
discontinuation_reason,
action_taken
)
select
e.patient_id, e.uuid, e.creator, e.visit_id, e.encounter_time, e.encounter_id, e.location_id,
max(if(o.concept_id = 164073, o.value_time, null )) as ipt_due_,
max(if(o.concept_id = 164074, o.value_time, null )) as _collected_ipt,
max(if(o.concept_id = 159098, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as hepatotoxity,
max(if(o.concept_id = 118983, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as peripheral_neuropathy,
max(if(o.concept_id = 512, (case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as rash,
max(if(o.concept_id = 164075, (case o.value_coded when 159407 then "Poor" when 159405 then "Good" when 159406 then "Fair" when 164077 then "Very Good" when 164076 then "Excellent" when 1067 then "Unknown" else "" end), "" )) as adherence,
max(if(o.concept_id = 160433, (case o.value_coded when 1267 then "Completed" when 5240 then "Lost to followup" when 159836 then "Discontinued" when 160034 then "Died" when 159492 then "Transferred Out" else "" end), "" )) as outcome,
max(if(o.concept_id = 1266, (case o.value_coded when 102 then "Drug Toxicity" when 112141 then "TB" when 5622 then "Other" else "" end), "" )) as discontinuation_reason,
max(if(o.concept_id = 160632, trim(o.value_text), "" )) as action_taken
from encounter e
inner join form f on f.form_id=e.form_id and f.uuid in ("22c68f86-bbf0-49ba-b2d1-23fa7ccf0259")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164073, 164074, 159098, 118983, 512, 164075, 160433, 1266, 160632) and o.voided=0
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing IPT followup forms", CONCAT("Time: ", NOW());
END$$

-- ------------- populate etl_ipt_followup-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ccc_defaulter_tracing$$
CREATE PROCEDURE sp_populate_etl_ccc_defaulter_tracing()
BEGIN
SELECT "Processing ccc defaulter tracing form", CONCAT("Time: ", NOW());

insert into kenyaemr_etl.etl_ccc_defaulter_tracing(
uuid,
provider,
patient_id,
visit_id,
visit_,
location_id,
encounter_id,
tracing_type,
tracing_outcome,
attempt_number,
is_final_trace,
true_status,
cause_of_death,
comments
)
select
e.uuid, e.creator, e.patient_id, e.visit_id, e.encounter_time, e.location_id, e.encounter_id,
max(if(o.concept_id = 164966, o.value_coded, null )) as tracing_type,
max(if(o.concept_id = 160721, o.value_coded, null )) as tracing_outcome,
max(if(o.concept_id = 1639, value_numeric, "" )) as attempt_number,
max(if(o.concept_id = 163725, o.value_coded, "" )) as is_final_trace,
max(if(o.concept_id = 160433, o.value_coded, "" )) as true_status,
max(if(o.concept_id = 1599, o.value_coded, "" )) as cause_of_death,
max(if(o.concept_id = 160716, o.value_text, "" )) as comments
from encounter e
inner join form f on f.form_id=e.form_id and f.uuid in ("a1a62d1e-2def-11e9-b210-d663bd873d93")
inner join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164966, 160721, 1639, 163725, 160433, 1599, 160716) and o.voided=0
where e.voided=0
group by e.encounter_id;
SELECT "Completed processing CCC defaulter tracing forms", CONCAT("Time: ", NOW());
END$$

-- ------------- populate etl_ART_preparation-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_ART_preparation $$
CREATE PROCEDURE sp_populate_etl_ART_preparation()
  BEGIN
    SELECT "Processing ART Preparation ", CONCAT("Time: ", NOW());
    insert into kenyaemr_etl.etl_ART_preparation(

uuid,
patient_id,
visit_id,
visit_,
location_id,
encounter_id,
provider,
understands_hiv_art_benefits,
screened_negative_substance_abuse,
screened_negative_psychiatric_illness,
HIV_status_disclosure,
trained_drug_admin,
informed_drug_side_effects,
caregiver_committed,
adherance_barriers_identified,
caregiver_location_contacts_known,
ready_to_start_art,
identified_drug_time,
treatment_supporter_engaged,
support_grp_meeting_awareness,
enrolled_in_reminder_system,
other_support_systems

)
    select
   e.uuid,
   e.patient_id,
   e.visit_id,
   e.encounter_time,
   e.location_id,
   e.encounter_id,
   e.creator,
   max(if(o.concept_id=1729,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as understands_hiv_art_benefits,
   max(if(o.concept_id=160246,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as screened_negative_substance_abuse,
   max(if(o.concept_id=159891,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as screened_negative_psychiatric_illness,
   max(if(o.concept_id=1048,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end), "" )) as HIV_status_disclosure,
   max(if(o.concept_id=164425,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as trained_drug_admin,
   max(if(o.concept_id=121764,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end), "" )) as informed_drug_side_effects,
   max(if(o.concept_id=5619,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as caregiver_committed,
   max(if(o.concept_id=159707,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as adherance_barriers_identified,
   max(if(o.concept_id=163089,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as caregiver_location_contacts_given,
   max(if(o.concept_id=162695,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as ready_to_start_art,
   max(if(o.concept_id=160119,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as identified_drug_time,
   max(if(o.concept_id=164886,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as treatment_supporter_engaged,
   max(if(o.concept_id=163766,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as support_grp_meeting_awareness,
   max(if(o.concept_id=163164,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as enrolled_in_reminder_system,
   max(if(o.concept_id=164360,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as other_support_systems
    from encounter e
   inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
 and o.concept_id in(1729,160246,159891,1048,164425,121764,5619,159707,163089,162695,160119,164886,163766,163164,164360)
   inner join
     (
     select form_id, uuid,name from openmrs.form where
 uuid in('782a4263-3ac9-4ce8-b316-534571233f12')
     ) f on f.form_id= e.form_id
   left join (
     select
    o.person_id,
    o.encounter_id,
    o.obs_group_id
     from obs o
    inner join encounter e on e.encounter_id = o.encounter_id
    inner join openmrs.form f on f.form_id=e.form_id and f.uuid in ('782a4263-3ac9-4ce8-b316-534571233f12')
     where o.voided=0
     group by e.encounter_id, o.obs_group_id
     ) t on e.encounter_id = t.encounter_id
    group by e.encounter_id;
    SELECT "Completed processing ART Preparation ", CONCAT("Time: ", NOW());
    END$$

-- ------------- populate etl_enhanced_adherence-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_enhanced_adherence $$
CREATE PROCEDURE sp_populate_etl_enhanced_adherence()
	BEGIN
		SELECT "Processing Enhanced Adherence ", CONCAT("Time: ", NOW());
		insert into kenyaemr_etl.etl_enhanced_adherence(
			uuid,
			patient_id,
			visit_id,
			visit_,
			location_id,
			encounter_id,
			provider,
			session_number,
			first_session_,
			pill_count,
			arv_adherence,
			has_vl_results,
			vl_results_suppressed,
			vl_results_feeling,
			cause_of_high_vl,
			way_forward,
			patient_hiv_knowledge,
			patient_drugs_uptake,
			patient_drugs_reminder_tools,
			patient_drugs_uptake_during_travels,
			patient_drugs_side_effects_response,
			patient_drugs_uptake_most_difficult_times,
			patient_drugs_daily_uptake_feeling,
			patient_ambitions,
			patient_has_people_to_talk,
			patient_enlisting_social_support,
			patient_income_sources,
			patient_challenges_reaching_clinic,
			patient_worried_of_accidental_disclosure,
			patient_treated_differently,
			stigma_hinders_adherence,
			patient_tried_faith_healing,
			patient_adherence_improved,
			patient_doses_missed,
			review_and_barriers_to_adherence,
			other_referrals,
			appointments_honoured,
			referral_experience,
			home_visit_benefit,
			adherence_plan,
			next_appointment_
		)
			select
				e.uuid,
				e.patient_id,
				e.visit_id,
				e.encounter_time,
				e.location_id,
				e.encounter_id,
				e.creator,
				max(if(o.concept_id=1639,o.value_numeric,null)) as session_number,
				max(if(o.concept_id=164891,o.value_time,null)) as first_session_,
				max(if(o.concept_id=162846,o.value_numeric,null)) as pill_count,
				max(if(o.concept_id=1658,(case o.value_coded when 159405 then "Good" when 163794 then "Inadequate" when 159407 then "Poor" else "" end), "" )) as arv_adherence,
				max(if(o.concept_id=164848,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as has_vl_results,
				max(if(o.concept_id=163310,(case o.value_coded when 1302 then "Suppressed" when 1066 then "Unsuppresed" else "" end), "" )) as vl_results_suppressed,
				max(if(o.concept_id=164981,trim(o.value_text),null)) as vl_results_feeling,
				max(if(o.concept_id=164982,trim(o.value_text),null)) as cause_of_high_vl,
				max(if(o.concept_id=160632,trim(o.value_text),null)) as way_forward,
				max(if(o.concept_id=164983,trim(o.value_text),null)) as patient_hiv_knowledge,
				max(if(o.concept_id=164984,trim(o.value_text),null)) as patient_drugs_uptake,
				max(if(o.concept_id=164985,trim(o.value_text),null)) as patient_drugs_reminder_tools,
				max(if(o.concept_id=164986,trim(o.value_text),null)) as patient_drugs_uptake_during_travels,
				max(if(o.concept_id=164987,trim(o.value_text),null)) as patient_drugs_side_effects_response,
				max(if(o.concept_id=164988,trim(o.value_text),null)) as patient_drugs_uptake_most_difficult_times,
				max(if(o.concept_id=164989,trim(o.value_text),null)) as patient_drugs_daily_uptake_feeling,
				max(if(o.concept_id=164990,trim(o.value_text),null)) as patient_ambitions,
				max(if(o.concept_id=164991,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_has_people_to_talk,
				max(if(o.concept_id=164992,trim(o.value_text),null)) as patient_enlisting_social_support,
				max(if(o.concept_id=164993,trim(o.value_text),null)) as patient_income_sources,
				max(if(o.concept_id=164994,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_challenges_reaching_clinic,
				max(if(o.concept_id=164995,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_worried_of_accidental_disclosure,
				max(if(o.concept_id=164996,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_treated_differently,
				max(if(o.concept_id=164997,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as stigma_hinders_adherence,
				max(if(o.concept_id=164998,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_tried_faith_healing,
				max(if(o.concept_id=1898,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as patient_adherence_improved,
				max(if(o.concept_id=160110,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end), "" )) as patient_doses_missed,
				max(if(o.concept_id=163108,trim(o.value_text),null)) as review_and_barriers_to_adherence,
				max(if(o.concept_id=1272,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as other_referrals,
				max(if(o.concept_id=164999,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as appointments_honoured,
				max(if(o.concept_id=165000,trim(o.value_text),null)) as referral_experience,
				max(if(o.concept_id=165001,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end), "" )) as home_visit_benefit,
				max(if(o.concept_id=165002,trim(o.value_text),null)) as adherence_plan,
				max(if(o.concept_id=5096,o.value_time,null)) as next_appointment_

			from encounter e
				inner join obs o on e.encounter_id = o.encounter_id and o.voided =0
																		and o.concept_id in(1639,164891,162846,1658,164848,163310,164981,164982,160632,164983,164984,164985,164986,164987,164988,164989,164990,164991,164992,164993,164994,164995,164996,164997,164998,1898,160110,163108,1272,164999,165000,165001,165002,5096)
				inner join
				(
					select form_id, uuid,name from openmrs.form where
						uuid in('c483f10f-d9ee-4b0d-9b8c-c24c1ec24701')
				) f on f.form_id= e.form_id
				left join (
										select
											o.person_id,
											o.encounter_id,
											o.obs_group_id
										from obs o
											inner join encounter e on e.encounter_id = o.encounter_id
											inner join openmrs.form f on f.form_id=e.form_id and f.uuid in ('c483f10f-d9ee-4b0d-9b8c-c24c1ec24701')
										where o.voided=0
										group by e.encounter_id, o.obs_group_id
									) t on e.encounter_id = t.encounter_id
			group by e.encounter_id;
		SELECT "Completed processing Enhanced Adherence ", CONCAT("Time: ", NOW());
		END$$


		SET sql_mode=@OLD_SQL_MODE$$

-- ------------------------------------------- running all procedures -----------------------------

DROP PROCEDURE IF EXISTS sp_first_time_setup$$
CREATE PROCEDURE sp_first_time_setup()
BEGIN
DECLARE populate_script_id ;
SELECT "Beginning first time setup", CONCAT("Time: ", NOW());
INSERT INTO kenyaemr_etl.etl_script_status(script_name, start_time) VALUES('initial_population_of_tables', NOW());
SET populate_script_id = LAST_INSERT_ID();

CALL sp_populate_etl_patient_demographics();
CALL sp_populate_etl_hiv_enrollment();
CALL sp_populate_etl_hiv_followup();
CALL sp_populate_etl_laboratory_extract();
CALL sp_populate_etl_pharmacy_extract();
CALL sp_populate_etl_program_discontinuation();
CALL sp_populate_etl_mch_enrollment();
CALL sp_populate_etl_mch_antenatal_visit();
CALL sp_populate_etl_mch_postnatal_visit();
CALL sp_populate_etl_tb_enrollment();
CALL sp_populate_etl_tb_follow_up_visit();
CALL sp_populate_etl_tb_screening();
CALL sp_populate_etl_hei_enrolment();
CALL sp_populate_etl_hei_immunization();
CALL sp_populate_etl_hei_follow_up();
CALL sp_populate_etl_mch_delivery();
CALL sp_populate_etl_mch_discharge();
CALL sp_drug_event();
CALL sp_populate_hts_test();
CALL sp_populate_hts_linkage_and_referral();
CALL sp_populate_etl_ipt_screening();
CALL sp_populate_etl_ipt_follow_up();
CALL sp_populate_etl_ccc_defaulter_tracing();
CALL sp_populate_etl_ART_preparation();
CALL sp_populate_etl_enhanced_adherence();
CALL sp_populate_etl_patient_triage();
CALL sp_up_dashboard_table();

UP kenyaemr_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed first time setup", CONCAT("Time: ", NOW());
END$$




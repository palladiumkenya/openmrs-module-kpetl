
SET @OLD_SQL_MODE=@@SQL_MODE$$
SET SQL_MODE=''$$

DROP PROCEDURE IF EXISTS sp_populate_etl_client_registration$$
CREATE PROCEDURE sp_populate_etl_client_registration()
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
           p.birthdate,
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
                p.birthdate,
                p.dead,
                p.voided,
                p.death_date
         from person p
                left join patient pa on pa.patient_id=p.person_id
                left join person_name pn on pn.person_id = p.person_id and pn.voided=0
         where p.voided=0
         GROUP BY p.person_id
         ) p
    ON DUPLICATE KEY UPDATE given_name = p.given_name, middle_name=p.middle_name, family_name=p.family_name;

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
          group by pi.patient_id) pid on pid.patient_id=r.client_id
    set
        r.national_id_number=pid.national_id,
        r.passport_number=pid.passport_number;

    update kp_etl.etl_client_registration r
    join (select pa.person_id as client_id,
                 pa.address1 as postal_address,
                 pa.county_district as county,
                 pa.state_province as sub_county,
                 pa.address6 as location,
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
    DROP PROCEDURE IF EXISTS sp_populate_etl_contact$$
    CREATE PROCEDURE sp_populate_etl_contact()
      BEGIN
        SELECT "Processing client contact data ", CONCAT("Time: ", NOW());
        insert into kp_etl.etl_contact (
            uuid,
            client_id,
            visit_id,
            visit_date,
            location_id,
            encounter_id,
            encounter_provider,
            date_created,
            key_population_type,
            contacted_by_peducator,
            program_name,
            frequent_hotspot_name,
            frequent_hotspot_type,
            year_started_sex_work,
            year_started_sex_with_men,
            year_started_drugs,
            avg_weekly_sex_acts,
            avg_weekly_anal_sex_acts,
            avg_daily_drug_injections,
            contact_person_name,
            contact_person_alias,
            contact_person_phone,
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
               max(if(o.concept_id=164929,(case o.value_coded when 165083 then "Female sex worker" when 160578 then "Male who have sex with Men" when 165084 then "Male sex worker" when 165085
                                                     then  "People who use drugs" when 105 then "People who inject drugs"  when  165108 then "Transgender"  when 165107 then "Transgender" else "" end),null)) as key_population_type,
               max(if(o.concept_id=165004,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as contacted_by_peducator,
               max(if(o.concept_id=165137,o.value_text,null)) as program_name,
               max(if(o.concept_id=165006,o.value_text,null)) as frequent_hotspot_name,
               max(if(o.concept_id=165005,( case o.value_coded
                                              when 165011 then "Street"
                                              when 165012 then "Injecting den"
                                              when 165013 then "Uninhabitable building"
                                              when 165014 then "Public Park"
                                              when 165015 then "Beach"
                                              when 165016 then "Casino"
                                              when 165017 then "Bar with lodging"
                                              when 165018 then "Bar without lodging"
                                              when 165019 then "Sex den"
                                              when 165020 then "Strip club"
                                              when 165021 then "Highway"
                                              when 165022 then "Brothel"
                                              when 165023 then "Guest house/hotel"
                                              when 165025 then "illicit brew den"
                                              when 165026 then "Barber shop/salon" else "" end),null)) as frequent_hotspot_type,
               max(if(o.concept_id=165030,o.value_numeric,null)) as year_started_sex_work,
               max(if(o.concept_id=165031,o.value_numeric,null)) as year_started_sex_with_men,
               max(if(o.concept_id=165032,o.value_numeric,null)) as year_started_drugs,
               max(if(o.concept_id=165007,o.value_numeric,null)) as avg_weekly_sex_acts,
               max(if(o.concept_id=165008,o.value_numeric,null)) as avg_weekly_anal_sex_acts,
               max(if(o.concept_id=165009,o.value_numeric,null)) as avg_weekly_drug_injections,
               max(if(o.concept_id=160638,o.value_text,null)) as contact_person_name,
               max(if(o.concept_id=165038,o.value_text,null)) as contact_person_alias,
               max(if(o.concept_id=160642,o.value_text,null)) as contact_person_phone,
               e.voided
        from openmrs.encounter e
               inner join
                 (
                 select encounter_type_id, uuid, name from openmrs.encounter_type where uuid='ea68aad6-4655-4dc5-80f2-780e33055a9e'
                 ) et on et.encounter_type_id=e.encounter_type
               join openmrs.patient p on p.patient_id=e.patient_id and p.voided=0
               left outer join openmrs.obs o on o.encounter_id=e.encounter_id and o.voided=0
                                          and o.concept_id in (164929,165004,165137,165006,165005,165030,165031,165032,165007,165008,165009,160638,165038,160642)
        where e.voided=0
        group by e.patient_id, e.encounter_id;

        SELECT "Completed processing KP contact data", CONCAT("Time: ", NOW());

        update kp_etl.etl_contact c
        join (select pi.patient_id,
                     max(if(pit.uuid='b7bfefd0-239b-11e9-ab14-d663bd873d93',pi.identifier,null)) unique_identifier
              from openmrs.patient_identifier pi
                     join openmrs.patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
              where voided=0
              group by pi.patient_id) pid on pid.patient_id=c.client_id
        set
            c.unique_identifier=pid.unique_identifier;

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
                   max(if(o.concept_id=165004,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as contacted_for_prevention,
                   max(if(o.concept_id=165027,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as has_regular_free_sex_partner,
                   max(if(o.concept_id=165030,o.value_numeric,null)) as year_started_sex_work,
                   max(if(o.concept_id=165031,o.value_numeric,null)) as year_started_sex_with_men,
                   max(if(o.concept_id=165032,o.value_numeric,null)) as year_started_drugs,
                   max(if(o.concept_id=123160,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as has_expereienced_sexual_violence,
                   max(if(o.concept_id=165034,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as has_expereienced_physical_violence,
                   max(if(o.concept_id=164401,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as ever_tested_for_hiv,
                   max(if(o.concept_id=164956,(case o.value_coded when 163722 then "Rapid HIV Testing" when 164952 THEN "Self Test" else "" end),null)) as ever_tested_for_hiv,
                   max(if(o.concept_id=165153,(case o.value_coded when 703 then "Yes I tested positive" when 664 THEN "Yes I tested negative" when 1066 THEN "No I do not want to share" else "" end),null)) as share_test_results,
                   max(if(o.concept_id=165154,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as willing_to_test,
                   max(if(o.concept_id=159803,o.value_text,null)) as test_decline_reason,
                   max(if(o.concept_id=159811,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as receiving_hiv_care,
                   max(if(o.concept_id=162724,o.value_text,null)) as care_facility_name,
                   max(if(o.concept_id=162053,o.value_numeric,null)) as ccc_number,
                   max(if(o.concept_id=164437,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as vl_test_done,
                   max(if(o.concept_id=163281,o.value_datetime,null)) as vl_results_date,
                   max(if(o.concept_id=165036,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as contact_for_appointment,
                   max(if(o.concept_id=164966,(case o.value_coded when 161642 then "Treatment supporter" when 165037 then "Peer educator"  when 1555 then "Outreach worker"
                                                                  when 159635 then "Phone number" else "" end),null)) as contact_method,
                   max(if(o.concept_id=160638,o.value_text,null)) as buddy_name,
                   max(if(o.concept_id=160642,o.value_text,null)) as buddy_phone_number,
                   e.voided
            from encounter e
                   inner join
                     (
                     select encounter_type_id, uuid, name from encounter_type where uuid='c7f47a56-207b-11e9-ab14-d663bd873d93'
                     ) et on et.encounter_type_id=e.encounter_type
                   join patient p on p.patient_id=e.patient_id and p.voided=0
                   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                              and o.concept_id in (165004,165027,165030,165031,165032,123160,165034,164401,164956,165153,165154,159803,159811,
                    162724,162053,164437,163281,165036,164966,160638,160642)
            where e.voided=0
            group by e.patient_id, e.encounter_id;
            SELECT "Completed processing KP client enrollment data", CONCAT("Time: ", NOW());
            END$$


            -- ------------- populate etl_clinical_visit--------------------------------

            DROP PROCEDURE IF EXISTS sp_populate_etl_clinical_visit$$
            CREATE PROCEDURE sp_populate_etl_clinical_visit()
              BEGIN
                SELECT "Processing Clinical Visit ", CONCAT("Time: ", NOW());
                INSERT INTO kp_etl.etl_clinical_visit(
                    uuid,
                    client_id,
                    visit_id,
                    visit_date,
                    location_id,
                    encounter_id,
                    encounter_provider,
                    date_created,
                    implementing_partner,
                    type_of_visit,
                    visit_reason,
                    service_delivery_model,
                    sti_screened,
                    sti_results,
                    sti_treated,
                    sti_referred,
                    sti_referred_text,
                    tb_screened,
                    tb_results,
                    tb_treated,
                    tb_referred,
                    tb_referred_text,
                    hepatitisB_screened,
                    hepatitisB_results,
                    hepatitisB_treated,
                    hepatitisB_referred,
                    hepatitisB_text,
                    hepatitisC_screened,
                    hepatitisC_results,
                    hepatitisC_treated,
                    hepatitisC_referred,
                    hepatitisC_text,
                    overdose_screened,
                    overdose_results,
                    overdose_treated,
                    received_naloxone,
                    overdose_referred,
                    overdose_text,
                    abscess_screened,
                    abscess_results,
                    abscess_treated,
                    abscess_referred,
                    abscess_text,
                    alcohol_screened,
                    alcohol_results,
                    alcohol_treated,
                    alcohol_referred,
                    alcohol_text,
                    cerv_cancer_screened,
                    cerv_cancer_results,
                    cerv_cancer_treated,
                    cerv_cancer_referred,
                    cerv_cancer_text,
                    prep_screened,
                    prep_results,
                    prep_treated,
                    prep_referred,
                    prep_text,
                    violence_screened,
                    violence_results,
                    violence_treated,
                    violence_referred,
                    violence_text,
                    risk_red_counselling_screened,
                    risk_red_counselling_eligibility,
                    risk_red_counselling_support,
                    risk_red_counselling_ebi_provided,
                    risk_red_counselling_text,
                    fp_screened,
                    fp_eligibility,
                    fp_treated,
                    fp_referred,
                    fp_text,
                    mental_health_screened,
                    mental_health_results,
                    mental_health_support,
                    mental_health_referred,
                    mental_health_text,
                    hiv_self_rep_status,
                    last_hiv_test_setting,
                    counselled_for_hiv,
                    hiv_tested,
                    test_frequency,
                    received_results,
                    test_results,
                    linked_to_art,
                    facility_linked_to,
                    self_test_education,
                    self_test_kits_given,
                    self_use_kits,
                    distribution_kits,
                    self_tested,
                    self_test_date,
                    self_test_frequency,
                    self_test_results,
                    test_confirmatory_results,
                    confirmatory_facility,
                    offsite_confirmatory_facility,
                    self_test_linked_art,
                    self_test_link_facility,
                    hiv_care_facility,
                    other_hiv_care_facility,
                    initiated_art_this_month,
                    active_art,
                    eligible_vl,
                    vl_test_done,
                    vl_results,
                    received_vl_results,
                    condom_use_education,
                    post_abortal_care,
                    linked_to_psychosocial,
                    male_condoms_no,
                    female_condoms_no,
                    lubes_no,
                    syringes_needles_no,
                    pep_eligible,
                    exposure_type,
                    other_exposure_type,
                    clinical_notes,
                    appointment_date,
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
                       max(if(o.concept_id=165347,o.value_text,null)) as implementing_partner,
                       max(if(o.concept_id=164181,(case o.value_coded when 162080 then "Initial" when 164142 THEN "Revisit" else "" end),null)) as type_of_visit,
                       max(if(o.concept_id=164082,(case o.value_coded when 5006 then "Asymptomatic" when 1068 THEN "Symptomatic" when 165348 then "Quarterly Screening checkup" when 160523 then "Follow up"  else "" end),null)) as visit_reason,
                       max(if(o.concept_id=160540,(case o.value_coded when 161235 then "Static" when 160545 THEN "Outreach" else "" end),null)) as service_delivery_model,
                       max(if(o.concept_id=161558,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as sti_screened,
                       max(if(o.concept_id=165199,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as sti_results,
                       max(if(o.concept_id=165200,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as sti_treated,
                       max(if(o.concept_id=165249,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as sti_referred,
                       max(if(o.concept_id=165250,o.value_text,null)) as sti_referred_text,
                       max(if(o.concept_id=165197,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as tb_screened,
                       max(if(o.concept_id=165198,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as tb_results,
                       max(if(o.concept_id=1111,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "NA" end),null)) as tb_treated,
                       max(if(o.concept_id=162310,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as tb_referred,
                       max(if(o.concept_id=163323,o.value_text,null)) as tb_referred_text,
                       max(if(o.concept_id=165040,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as hepatitisB_screened,
                       max(if(o.concept_id=1322,(case o.value_coded when 664 then "N" when 703 THEN "P" else "" end),null)) as hepatitisB_results,
                       max(if(o.concept_id=165251,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "NA" end),null)) as hepatitisB_treated,
                       max(if(o.concept_id=165252,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as hepatitisB_referred,
                       max(if(o.concept_id=165253,o.value_text,null)) as hepatitisB_text,
                       max(if(o.concept_id=165041,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as hepatitisC_screened,
                       max(if(o.concept_id=161471,(case o.value_coded when 664 then "N" when 703 THEN "P" else "" end),null)) as hepatitisC_results,
                       max(if(o.concept_id=165254,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "NA" end),null)) as hepatitisC_treated,
                       max(if(o.concept_id=165255,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as hepatitisC_referred,
                       max(if(o.concept_id=165256,o.value_text,null)) as hepatitisC_text,
                       max(if(o.concept_id=165042,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as overdose_screened,
                       max(if(o.concept_id=165046,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as overdose_results,
                       max(if(o.concept_id=165257,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as overdose_treated,
                       max(if(o.concept_id=165201,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as received_naloxone,
                       max(if(o.concept_id=165258,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as overdose_referred,
                       max(if(o.concept_id=165259,o.value_text,null)) as overdose_text,
                       max(if(o.concept_id=165044,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as abscess_screened,
                       max(if(o.concept_id=165051,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as abscess_results,
                       max(if(o.concept_id=165260,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as abscess_treated,
                       max(if(o.concept_id=165261,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as abscess_referred,
                       max(if(o.concept_id=165262,o.value_text,null)) as abscess_text,
                       max(if(o.concept_id=165043,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as alcohol_screened,
                       max(if(o.concept_id=165047,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as alcohol_results,
                       max(if(o.concept_id=165263,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as alcohol_treated,
                       max(if(o.concept_id=165264,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as alcohol_referred,
                       max(if(o.concept_id=165265,o.value_text,null)) as alcohol_text,
                       max(if(o.concept_id=164934,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as cerv_cancer_screened,
                       max(if(o.concept_id=165196,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as cerv_cancer_results,
                       max(if(o.concept_id=165266,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as cerv_cancer_treated,
                       max(if(o.concept_id=165267,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as cerv_cancer_referred,
                       max(if(o.concept_id=165268,o.value_text,null)) as cerv_cancer_text,
                       max(if(o.concept_id=165076,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as prep_screened,
                       max(if(o.concept_id=165202,(case o.value_coded when 165087 then "Eligible" when 165078 THEN "Not eligible" else "" end),null)) as prep_results,
                       max(if(o.concept_id=165203,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as prep_treated,
                       max(if(o.concept_id=165270,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as prep_referred,
                       max(if(o.concept_id=165271,o.value_text,null)) as prep_text,
                       max(if(o.concept_id=165204,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as violence_screened,
                       max(if(o.concept_id=165205,(case o.value_coded when 165206 then "Harrasment" when 165207 THEN "Illegal arrest" when 123007 THEN "Verbal Abuse" when 127910 THEN "Rape/Sexual assault" when 126312 THEN "Discrimination"  else "" end),null)) as violence_results,
                       max(if(o.concept_id=165208,(case o.value_coded when  1065 then "Supported" when 1066 THEN "Not supported" else "" end),null)) as violence_treated,
                       max(if(o.concept_id=165273,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as violence_referred,
                       max(if(o.concept_id=165274,o.value_text,null)) as violence_text,
                       max(if(o.concept_id=165045,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as risk_red_counselling_screened,
                       max(if(o.concept_id=165050,(case o.value_coded when 165087 then "Eligible" when 165078 THEN "Not eligible" else "" end),null)) as risk_red_counselling_eligibility,
                       max(if(o.concept_id=165053,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as risk_red_counselling_support,
                       max(if(o.concept_id=161595,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as risk_red_counselling_ebi_provided,
                       max(if(o.concept_id=165277,o.value_text,null)) as risk_red_counselling_text,
                       max(if(o.concept_id=1382,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as fp_screened,
                       max(if(o.concept_id=165209,(case o.value_coded when 165087 then "Eligible" when 165078 THEN "Not eligible" else "" end),null)) as fp_eligibility,
                       max(if(o.concept_id=160653,(case o.value_coded when 1065 then "Y" when 1066 THEN "N" else "" end),null)) as fp_treated,
                       max(if(o.concept_id=165279,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as fp_referred,
                       max(if(o.concept_id=165280,o.value_text,null)) as fp_text,
                       max(if(o.concept_id=165210,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as mental_health_screened,
                       max(if(o.concept_id=165211,(case o.value_coded when 165212 then "Depression unlikely" when 157790 THEN "Mild depression" when 134017 THEN "Moderate depression" when 134011 THEN "Moderate-severe depression" when 126627 THEN "Severe Depression"  else "" end),null)) as mental_health_results,
                       max(if(o.concept_id=165213,(case o.value_coded when 1065 then "Supported" when 1066 THEN "Not supported" else "" end),null)) as mental_health_support,
                       max(if(o.concept_id=165281,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as mental_health_referred,
                       max(if(o.concept_id=165282,o.value_text,null)) as mental_health_text,
                       max(if(o.concept_id=165214,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" when 1067 then "Unknown" else "" end),null)) as hiv_self_rep_status,
                       max(if(o.concept_id=165215,(case o.value_coded when 165216 then "Universal HTS" when 165217 THEN "Self-testing" when 1402 then "Never tested" else "" end),null)) as last_hiv_test_setting,
                       max(if(o.concept_id=159382,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as counselled_for_hiv,
                       max(if(o.concept_id=164401,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" when 1175 then "Not Applicable" else "" end),null)) as hiv_tested,
                       max(if(o.concept_id=165218,(case o.value_coded when 162080 THEN "Initial" when 162081 then "Repeat" when 1175 then "Not Applicable" else "" end),null)) as test_frequency,
                       max(if(o.concept_id=164848,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" when 1067 then "Not Applicable" else "" end),null)) as received_results,
                       max(if(o.concept_id=159427,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" when 165232 then "Inconclusive" when 138571 then "Known Positive" when 1118 then "Not done" else "" end),null)) as test_results,
                       max(if(o.concept_id=1648,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end),null)) as linked_to_art,
                       max(if(o.concept_id=163042,o.value_text,null)) as facility_linked_to,
                       max(if(o.concept_id=165220,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as self_test_education,
                       max(if(o.concept_id=165221,(case o.value_coded when 165222 then "Self use" when 165223 THEN "Distribution" else "" end),null)) as self_test_kits_given,
                       max(if(o.concept_id=165222,o.value_text,null)) as self_use_kits,
                       max(if(o.concept_id=165223,o.value_text,null)) as distribution_kits,
                       max(if(o.concept_id=164952,(case o.value_coded when 1065 THEN "Y" when 1066 then "N" else "" end),null)) as self_tested,
                       max(if(o.concept_id=164400,o.value_datetime,null)) as self_test_date,
                       max(if(o.concept_id=165231,(case o.value_coded when 162080 THEN "Initial" when 162081 then "Repeat" else "" end),null)) as self_test_frequency,
                       max(if(o.concept_id=165233,(case o.value_coded when 664 THEN "Negative" when 703 then "Positive" when 165232 then "Inconclusive" else "" end),null)) as self_test_results,
                       max(if(o.concept_id=165234,(case o.value_coded when 664 THEN "Negative" when 703 then "Positive" when 1118 then "Not done" else "" end),null)) as test_confirmatory_results,
                       max(if(o.concept_id=165237,o.value_text,null)) as confirmatory_facility,
                       max(if(o.concept_id=162724,o.value_text,null)) as offsite_confirmatory_facility,
                       max(if(o.concept_id=165238,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end),null)) as self_test_linked_art,
                       max(if(o.concept_id=161562,o.value_text,null)) as self_test_link_facility,
                       max(if(o.concept_id=165239,(case o.value_coded when 163266 THEN "Provided here" when 162723 then "Provided elsewhere" when 160563 then "Referred" else "" end),null)) as hiv_care_facility,
                       max(if(o.concept_id=163042,o.value_text,null)) as other_hiv_care_facility,
                       max(if(o.concept_id=165240,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" when 1175 then "Not Applicable" else "" end),null)) as initiated_art_this_month,
                       max(if(o.concept_id=160119,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" when 1175 then "Not Applicable" else "" end),null)) as active_art,
                       max(if(o.concept_id=165242,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" when 1175 then "Not Applicable" else "" end),null)) as eligible_vl,
                       max(if(o.concept_id=165243,(case o.value_coded when 1065 THEN "Y" when 1066 then "N" when 1175 then "Not Applicable" else "" end),null)) as vl_test_done,
                       max(if(o.concept_id=165246,(case o.value_coded when 165244 THEN "Y" when 165245 then "N" when 1175 then "NA" else "" end),null)) as vl_results,
                       max(if(o.concept_id=165246,(case o.value_coded when 164369 then "N"  else "Y" end),null)) as received_vl_results,
                       max(if(o.concept_id=165247,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as condom_use_education,
                       max(if(o.concept_id=164820,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as post_abortal_care,
                       max(if(o.concept_id=165302,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as linked_to_psychosocial,
                       max(if(o.concept_id=165055,o.value_numeric,null)) as male_condoms_no,
                       max(if(o.concept_id=165056,o.value_numeric,null)) as female_condoms_no,
                       max(if(o.concept_id=165057,o.value_numeric,null)) as lubes_no,
                       max(if(o.concept_id=165058,o.value_numeric,null)) as syringes_needles_no,
                       max(if(o.concept_id=164845,(case o.value_coded when 1065 THEN "Y" when 1066 then "N" else "NA" end),null)) as pep_eligible,
                       max(if(o.concept_id=165060,(case o.value_coded when 127910 THEN "Rape" when 165045 then "Condom burst" when 5622 then "Others" else "" end),null)) as exposure_type,
                       max(if(o.concept_id=163042,o.value_text,null)) as other_exposure_type,
                       max(if(o.concept_id=165248,o.value_text,null)) as clinical_notes,
                       max(if(o.concept_id=5096,o.value_datetime,null)) as appointment_date,
                       e.voided as voided
                from encounter e
                       inner join
                         (
                         select encounter_type_id, uuid, name from encounter_type where uuid in('92e03f22-9686-11e9-bc42-526af7764f64')
                         ) et on et.encounter_type_id=e.encounter_type
                       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                                  and o.concept_id in (165347,164181,164082,160540,161558,165199,165200,165249,165250,165197,165198,1111,162310,163323,165040,1322,165251,165252,165253,
                        165041,161471,165254,165255,165256,165042,165046,165257,165201,165258,165259,165044,165051,165260,165261,165262,165043,165047,165263,165264,165265,
                        164934,165196,165266,165267,165268,165076,165202,165203,165270,165271,165204,165205,165208,165273,165274,165045,165050,165053,161595,165277,1382,
                        165209,160653,165279,165280,165210,165211,165213,165281,165282,165214,165215,159382,164401,165218,164848,159427,1648,163042,165220,165221,165222,165223,
                        164952,164400,165231,165233,165234,165237,162724,165238,161562,165239,163042,165240,160119,165242,165243,165246,165247,164820,165302,165055,165056,
                        165057,165058,164845,165248,5096)
                where e.voided=0
                group by e.patient_id, e.encounter_id, visit_date;
                SELECT "Completed processing Clinical visit data ", CONCAT("Time: ", NOW());
                END$$

            -- ------------- populate etl_sti_treatment--------------------------------

                DROP PROCEDURE IF EXISTS sp_populate_etl_sti_treatment$$
                CREATE PROCEDURE sp_populate_etl_sti_treatment()
                  BEGIN
                    SELECT "Processing STI Treatment ", CONCAT("Time: ", NOW());
                    INSERT INTO kp_etl.etl_sti_treatment(
                        uuid,
                        client_id,
                        visit_id,
                        visit_date,
                        location_id,
                        encounter_id,
                        encounter_provider,
                        date_created,
                        visit_reason,
                        syndrome,
                        other_syndrome,
                        drug_prescription,
                        other_drug_prescription,
                        genital_exam_done,
                        lab_referral,
                        lab_form_number,
                        referred_to_facility,
                        facility_name,
                        partner_referral_done,
                        given_lubes,
                        no_of_lubes,
                        given_condoms,
                        no_of_condoms,
                        provider_comments,
                        provider_name,
                        appointment_date,
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
                           max(if(o.concept_id=164082,(case o.value_coded when 1068 THEN "Symptomatic" when 5006 then "Asymptomatic" when 163139 then "Quartely Screening" when 160523 then "Follow up" else "" end),null)) as visit_reason,
                           max(if(o.concept_id=1169,(case o.value_coded when 1065 then "Positive" when 1066 then "Negative" else "" end),null)) as syndrome,
                           max(if(o.concept_id=165138,o.value_text,null)) as other_syndrome,
                           max(if(o.concept_id=165200,(case o.value_coded when 1065 then "Yes" when 1066 then "No"
                                                                        else "" end),null)) as drug_prescription,
                           max(if(o.concept_id=163101,o.value_text,null)) as other_drug_prescription,
                           max(if(o.concept_id=163743,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as genital_exam_done,
                           max(if(o.concept_id=1272,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as lab_referral,
                           max(if(o.concept_id=163042,o.value_text,null)) as lab_form_number,
                           max(if(o.concept_id=1788,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as referred_to_facility,
                           max(if(o.concept_id=162724,o.value_text,null)) as facility_name,
                           max(if(o.concept_id=165128,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as partner_referral_done,
                           max(if(o.concept_id=165127,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as given_lubes,
                           max(if(o.concept_id=163169,o.value_numeric,null)) as no_of_lubes,
                           max(if(o.concept_id=159777,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as given_condoms,
                           max(if(o.concept_id=165055,o.value_numeric,null)) as no_of_condoms,
                           max(if(o.concept_id=162749,o.value_text,null)) as provider_comments,
                           max(if(o.concept_id=1473,o.value_text,null)) as provider_name,
                           max(if(o.concept_id=5096,o.value_datetime,null)) as appointment_date,
                           e.voided as voided
                    from encounter e
                           inner join
                             (
                             select encounter_type_id, uuid, name from encounter_type where uuid in('2cc8c535-bbfa-4668-98c7-b12e3550ee7b')
                             ) et on et.encounter_type_id=e.encounter_type
                           left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                                      and o.concept_id in (164082,1169,165138,165200,163101,163743,1272,163042,1788,162724,165128,165127,163169,
                            159777,165055,162749,1473,5096)
                    where e.voided=0
                    group by e.patient_id, e.encounter_id, visit_date;
                    SELECT "Completed processing STI Treatment data ", CONCAT("Time: ", NOW());

           END$$

-- ------------- populate etl_peer_calendar--------------------------------

                     DROP PROCEDURE IF EXISTS sp_populate_etl_peer_calendar$$
                     CREATE PROCEDURE sp_populate_etl_peer_calendar()
                       BEGIN
                         SELECT "Processing Peer calendar ", CONCAT("Time: ", NOW());
                         INSERT INTO  kp_etl.etl_peer_calendar(
                             uuid,
                             client_id,
                             visit_id,
                             visit_date,
                             location_id,
                             encounter_id,
                             encounter_provider,
                             date_created,
                             hotspot_name,
                             typology,
                             other_hotspots,
                             weekly_sex_acts,
                             monthly_condoms_required,
                             weekly_anal_sex_acts,
                             monthly_lubes_required,
                             daily_injections,
                             monthly_syringes_required,
                             years_in_sexwork_drugs,
                             experienced_violence,
                             service_provided_within_last_month,

                             week_1_monthly_n_and_s_distributed,
                             week_1_monthly_male_condoms_distributed,
                             week_1_monthly_lubes_distributed,
                             week_1_monthly_female_condoms_distributed,
                             week_1_monthly_self_test_kits_distributed,
                             week_1_received_clinical_service,
                             week_1_violence_reported,
                             week_1_referred,
                             week_1_health_edu,

                            week_2_monthly_n_and_s_distributed,
                             week_2_monthly_male_condoms_distributed,
                             week_2_monthly_lubes_distributed,
                             week_2_monthly_female_condoms_distributed,
                             week_2_monthly_self_test_kits_distributed,
                             week_2_received_clinical_service,
                             week_2_violence_reported,
                             week_2_referred,
                             week_2_health_edu,

                            week_3_monthly_n_and_s_distributed,
                             week_3_monthly_male_condoms_distributed,
                             week_3_monthly_lubes_distributed,
                             week_3_monthly_female_condoms_distributed,
                             week_3_monthly_self_test_kits_distributed,
                             week_3_received_clinical_service,
                             week_3_violence_reported,
                             week_3_referred,
                             week_3_health_edu,

                            week_4_monthly_n_and_s_distributed,
                             week_4_monthly_male_condoms_distributed,
                             week_4_monthly_lubes_distributed,
                             week_4_monthly_female_condoms_distributed,
                             week_4_monthly_self_test_kits_distributed,
                             week_4_received_clinical_service,
                             week_4_violence_reported,
                             week_4_referred,
                             week_4_health_edu,

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
                                max(if(o.concept_id=165006,o.value_text,null)) as hotspot_name,
                                max(if(o.concept_id=165005,o.value_coded,null)) as typology,
                                max(if(o.concept_id=165298,o.value_text,null)) as other_hotspots,
                                max(if(o.concept_id=165007,o.value_numeric,null)) as weekly_sex_acts,
                                max(if(o.concept_id=165299,o.value_numeric,null)) as monthly_condoms_required,
                                max(if(o.concept_id=165008,o.value_numeric,null)) as weekly_anal_sex_acts,
                                max(if(o.concept_id=165300,o.value_numeric,null)) as monthly_lubes_required,
                                max(if(o.concept_id=165009,o.value_numeric,null)) as daily_injections,
                                max(if(o.concept_id=165308,o.value_numeric,null)) as monthly_syringes_required,
                                max(if(o.concept_id=165301,o.value_numeric,null)) as years_in_sexwork_drugs,
                                max(if(o.concept_id=123160,o.value_coded,null)) as experienced_violence,
                                max(if(o.concept_id=165302,o.value_coded,null)) as service_provided_within_last_month,

                                max(if(o.concept_id=165304 and o.value_coded=165058,value_coded,null)) as week_1_monthly_n_and_s_distributed,
                               max(if(o.concept_id=165304 and o.value_coded=165055,value_coded,null)) as week_1_monthly_male_condoms_distributed,
                               max(if(o.concept_id=165304 and o.value_coded=165057,value_coded,null)) as week_1_monthly_lubes_distributed,
                               max(if(o.concept_id=165304 and o.value_coded=165056,value_coded,null)) as week_1_monthly_female_condoms_distributed,
                               max(if(o.concept_id=165304 and o.value_coded=165222,value_coded,null)) as week_1_monthly_self_test_kits_distributed,
                               max(if(o.concept_id=165304 and o.value_coded=1774,value_coded,null)) as week_1_received_clinical_service,
                               max(if(o.concept_id=165304 and o.value_coded=1774,value_coded,null)) as week_1_violence_reported,
                               max(if(o.concept_id=165304 and o.value_coded=1272,value_coded,null)) as week_1_referred,
                               max(if(o.concept_id=165304 and o.value_coded=165147,value_coded,null)) as week_1_health_edu,

                               max(if(o.concept_id=165305 and o.value_coded=165058,value_coded,null)) as week_2_monthly_n_and_s_distributed,
                               max(if(o.concept_id=165305 and o.value_coded=165055,value_coded,null)) as week_2_monthly_male_condoms_distributed,
                               max(if(o.concept_id=165305 and o.value_coded=165057,value_coded,null)) as week_2_monthly_lubes_distributed,
                               max(if(o.concept_id=165305 and o.value_coded=165056,value_coded,null)) as week_2_monthly_female_condoms_distributed,
                               max(if(o.concept_id=165305 and o.value_coded=165222,value_coded,null)) as week_2_monthly_self_test_kits_distributed,
                               max(if(o.concept_id=165305 and o.value_coded=1774,value_coded,null)) as week_2_received_clinical_service,
                               max(if(o.concept_id=165305 and o.value_coded=1774,value_coded,null)) as week_2_violence_reported,
                               max(if(o.concept_id=165305 and o.value_coded=1272,value_coded,null)) as week_2_referred,
                               max(if(o.concept_id=165305 and o.value_coded=165147,value_coded,null)) as week_2_health_edu,

                               max(if(o.concept_id=165306 and o.value_coded=165058,value_coded,null)) as week_3_monthly_n_and_s_distributed,
                               max(if(o.concept_id=165306 and o.value_coded=165055,value_coded,null)) as week_3_monthly_male_condoms_distributed,
                               max(if(o.concept_id=165306 and o.value_coded=165057,value_coded,null)) as week_3_monthly_lubes_distributed,
                               max(if(o.concept_id=165306 and o.value_coded=165056,value_coded,null)) as week_3_monthly_female_condoms_distributed,
                               max(if(o.concept_id=165306 and o.value_coded=165222,value_coded,null)) as week_3_monthly_self_test_kits_distributed,
                               max(if(o.concept_id=165306 and o.value_coded=1774,value_coded,null)) as week_3_received_clinical_service,
                               max(if(o.concept_id=165306 and o.value_coded=1774,value_coded,null)) as week_3_violence_reported,
                               max(if(o.concept_id=165306 and o.value_coded=1272,value_coded,null)) as week_3_referred,
                               max(if(o.concept_id=165306 and o.value_coded=165147,value_coded,null)) as week_3_health_edu,

                               max(if(o.concept_id=165307 and o.value_coded=165058,value_coded,null)) as week_4_monthly_n_and_s_distributed,
                               max(if(o.concept_id=165307 and o.value_coded=165055,value_coded,null)) as week_4_monthly_male_condoms_distributed,
                               max(if(o.concept_id=165307 and o.value_coded=165057,value_coded,null)) as week_4_monthly_lubes_distributed,
                               max(if(o.concept_id=165307 and o.value_coded=165056,value_coded,null)) as week_4_monthly_female_condoms_distributed,
                               max(if(o.concept_id=165307 and o.value_coded=165222,value_coded,null)) as week_4_monthly_self_test_kits_distributed,
                               max(if(o.concept_id=165307 and o.value_coded=1774,value_coded,null)) as week_4_received_clinical_service,
                               max(if(o.concept_id=165307 and o.value_coded=1774,value_coded,null)) as week_4_violence_reported,
                               max(if(o.concept_id=165307 and o.value_coded=1272,value_coded,null)) as week_4_referred,
                               max(if(o.concept_id=165307 and o.value_coded=165147,value_coded,null)) as week_4_health_edu,

                             max(if(o.concept_id=160632,o.value_text,null)) as remarks,
                                e.voided as voided
                         from encounter e
                                inner join
                                  (
                                  select encounter_type_id, uuid, name from encounter_type where uuid in('c4f9db39-2c18-49a6-bf9b-b243d673c64d')
                                  ) et on et.encounter_type_id=e.encounter_type
                                left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                                           and o.concept_id in (165304,165305,165306,165307,165006,165005,165298,165007,165299,165008,165301,165302,165341,165343,165057,165344,165345,
                                                           1774,123160,1749,165346,160632,165272)
                                               where e.voided=0
                         group by e.patient_id, e.encounter_id, visit_date;
                         SELECT "Completed processing Peer calendar data ", CONCAT("Time: ", NOW());
                         END$$

-- ------------------------------------ populate hts test table ----------------------------------------


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
             when 159939 then "Community Outreach"
             when 5622 then "Other"
             when 165350 then "Dice"
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

DROP PROCEDURE IF EXISTS sp_populate_violence_screening$$
CREATE PROCEDURE sp_populate_violence_screening()
BEGIN
SELECT "Processing violence screening";
	  INSERT INTO kp_etl.etl_violence_screening(
encounter_id,
client_id,
location_id,
visit_date,
visit_id,
encounter_provider,
date_created,
incident_place,
incident_date,
person_abused,
verbal_abuse,
physical_abuse,
discrimination,
sexual_abuse,
illegal_arrest,
harrassment,
local_gang_perpetrator,
police_perpetrator,
general_public_perpetrator,
client_perpetrator,
community_member_perpetrator,
local_authority_perpetrator,
health_care_provider_perpetrator,
education_institution_perpetrator,
religious_group_perpetrator,
drug_peddler_perpetrator,
pimp_perpetrator,
bar_owner_manager_perpetrator,
family_perpetrator,
partner_perpetrator,
neighbour_perpetrator,
employer_perpetrator,
other_perpetrator,
other_specified_perpetrator,
first_response_date,
violence_support_given,
prc_hts,
prc_hts_within_5_days,
prc_emergency_contraception,
prc_emergency_contraception_within_5_days,
prc_complaint_reg_at_police_stn,
prc_complaint_reg_at_police_stn_within_5_days,
prc_psychological_trauma_counselling,
prc_psychological_trauma_counselling_within_5_days,
prc_pep_duration,
prc_pep_within_5_days,
prc_sti_screening_treatment,
prc_sti_screening_treatment_within_5_days,
prc_legal_support,
prc_legal_support_within_5_days,
prc_medical_examination,
prc_medical_examination_within_5_days,
prc_form_file,
prc_form_file_within_5_days,
prc_other_services_given,
non_sexual_medical_services_and_care,
non_sexual_medical_services_and_care_within_5_days,
non_sexual_psychological_counselling,
non_sexual_psychological_counselling_within_5_days,
non_sexual_complaint_reg_at_police_stn,
non_sexual_complaint_reg_at_police_stn_within_5_days,
non_sexual_legal_support,
non_sexual_legal_support_within_5_days,
current_state_of_victim,
follow_up_plan,
issue_resolution_date,
voided
)
select
e.encounter_id,
e.patient_id as client_id,
e.location_id,
e.encounter_datetime as visit_date,
e.visit_id,
e.creator,
e.date_created,
max(if(o.concept_id=162725,o.value_text,null)) as incident_place,
max(if(o.concept_id=160753,o.value_datetime,null)) as incident_date,
max(if(o.concept_id=165164,o.value_coded,null)) as person_abused,
max(if(o.concept_id=165228 and o.value_coded=123007,value_coded,null)) as verbal_abuse,
max(if(o.concept_id=165228 and o.value_coded=152292,value_coded,null)) as physical_abuse,
max(if(o.concept_id=165228 and o.value_coded=126312,o.value_coded,null)) as discrimination,
max(if(o.concept_id=165228 and o.value_coded=152370,value_coded,null)) as sexual_abuse,
max(if(o.concept_id=165228 and o.value_coded=156761,value_coded,null)) as illegal_arrest,
max(if(o.concept_id=165228 and o.value_coded=165161,value_coded,null)) as harrassment,
max(if(o.concept_id=165283,o.value_coded,null)) as local_gang_perpetrator,
max(if(o.concept_id=165284,o.value_coded,null)) as police_perpetrator,
max(if(o.concept_id=165285,o.value_coded,null)) as general_public_perpetrator,
max(if(o.concept_id=165286,o.value_coded,null)) as client_perpetrator,
max(if(o.concept_id=163488,o.value_coded,null)) as community_member_perpetrator,
max(if(o.concept_id=165193,o.value_coded,null)) as local_authority_perpetrator,
max(if(o.concept_id=5619,o.value_coded,null)) as health_care_provider_perpetrator,
max(if(o.concept_id=165289,o.value_coded,null)) as education_institution_perpetrator,
max(if(o.concept_id=165290,o.value_coded,null)) as religious_group_perpetrator,
max(if(o.concept_id=165291,o.value_coded,null)) as drug_peddler_perpetrator,
max(if(o.concept_id=165292,o.value_coded,null)) as pimp_perpetrator,
max(if(o.concept_id=165293,o.value_coded,null)) as bar_owner_manager_perpetrator,
max(if(o.concept_id=1560,o.value_coded,null)) as family_perpetrator,
max(if(o.concept_id=165294,o.value_coded,null)) as partner_perpetrator,
max(if(o.concept_id=165295,o.value_coded,null)) as neighbour_perpetrator,
max(if(o.concept_id=165296,o.value_coded,null)) as employer_perpetrator,
max(if(o.concept_id=165241,o.value_coded,null)) as other_perpetrator,
max(if(o.concept_id=165230,o.value_text,null)) as other_specified_perpetrator,
max(if(o.concept_id=165349,o.value_datetime,null)) as first_response_date,
max(if(o.concept_id=165225,o.value_text,null)) as violence_support_given,
max(if(o.concept_id=159368,o.value_numeric,null)) as prc_hts,
max(if(o.concept_id=165165,o.value_coded,null)) as prc_hts_within_5_days,
max(if(o.concept_id=165166,o.value_numeric,null)) as prc_emergency_contraception,
max(if(o.concept_id=165167,o.value_coded,null)) as prc_emergency_contraception_within_5_days,
max(if(o.concept_id=165179,o.value_numeric,null)) as prc_complaint_reg_at_police_stn,
max(if(o.concept_id=165180,o.value_coded,null)) as prc_complaint_reg_at_police_stn_within_5_days,
max(if(o.concept_id=165168,o.value_numeric,null)) as prc_psychological_trauma_counselling,
max(if(o.concept_id=165169,o.value_coded,null)) as prc_psychological_trauma_counselling_within_5_days,
max(if(o.concept_id=165170,o.value_numeric,null)) as prc_pep_duration,
max(if(o.concept_id=165171,o.value_coded,null)) as prc_pep_within_5_days,
max(if(o.concept_id=165190,o.value_numeric,null)) as prc_sti_screening_treatment,
max(if(o.concept_id=165172,o.value_coded,null)) as prc_sti_screening_treatment_within_5_days,
max(if(o.concept_id=165173,o.value_numeric,null)) as prc_legal_support,
max(if(o.concept_id=165174,o.value_coded,null)) as prc_legal_support_within_5_days,
max(if(o.concept_id=165175,o.value_numeric,null)) as prc_medical_examination,
max(if(o.concept_id=165176,o.value_coded,null)) as prc_medical_examination_within_5_days,
max(if(o.concept_id=165178,o.value_numeric,null)) as prc_form_file,
max(if(o.concept_id=165177,o.value_coded,null)) as prc_form_file_within_5_days,
max(if(o.concept_id=163108,o.value_text,null)) as prc_other_services_given,
max(if(o.concept_id=165181,o.value_numeric,null)) as non_sexual_medical_services_and_care,
max(if(o.concept_id=165182,o.value_coded,null)) as non_sexual_medical_services_and_care_within_5_days,
max(if(o.concept_id=165183,o.value_numeric,null)) as non_sexual_psychological_counselling,
max(if(o.concept_id=165184,o.value_coded,null)) as non_sexual_psychological_counselling_within_5_days,
max(if(o.concept_id=165185,o.value_numeric,null)) as non_sexual_complaint_reg_at_police_stn,
max(if(o.concept_id=165186,o.value_coded,null)) as non_sexual_complaint_reg_at_police_stn_within_5_days,
max(if(o.concept_id=165187,o.value_numeric,null)) as non_sexual_legal_support,
max(if(o.concept_id=165188,o.value_coded,null)) as non_sexual_legal_support_within_5_days,
max(if(o.concept_id=165189,o.value_coded,null)) as current_state_of_victim,
max(if(o.concept_id=164378,o.value_text,null)) as follow_up_plan,
max(if(o.concept_id=165224,o.value_datetime,null)) as issue_resolution_date,
e.voided
from encounter e
   inner join
     (
     select encounter_type_id, uuid, name from encounter_type where uuid ='7b69daf5-b567-4384-9d29-f020c408d613'
     ) et on et.encounter_type_id=e.encounter_type
   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                              and o.concept_id in (162725,160753,165164,165228,165283,165284,165285,165286,163488,165193,5619,
                              165289,165290,165291,165292,165293,1560,165294,165295,165296,165241,5622,165230,165349,165225,159368,165165,
                              165166,165167,165179,165180,165168,165169,165170,165171,165190,165172,165173,165174,165175,165176,165178,
                              165177,163108,165181,165182,165183,165184,165185,165186,165187,165188,165189,164378,165224)
                  where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing violence screening";
END$$

 DROP PROCEDURE IF EXISTS sp_populate_etl_peer_tracking$$
             CREATE PROCEDURE sp_populate_etl_peer_tracking()
               BEGIN
                 SELECT "Processing peer tracking ", CONCAT("Time: ", NOW());
                 insert into kp_etl.etl_peer_tracking (
                     uuid,
                     client_id,
                     visit_id,
                     visit_date,
                     location_id,
                     encounter_id,
                     encounter_provider,
                     date_created,
                     tracing_attempted,
                     tracing_not_attempted_reason,
                     attempt_number,
                     tracing_date,
                     tracing_type,
                     tracing_outcome,
                     is_final_trace,
                     tracing_outcome_status,
                     voluntary_exit_comment,
                     status_in_program,
                     source_of_information,
                     other_informant,
                     date_last_modified,
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
                      max(if(o.concept_id=165004, o.value_coded ,null)) as tracing_attempted,
                      max(if(o.concept_id=165071, o.value_coded ,null)) as tracing_not_attempted_reason,
                      max(if(o.concept_id=1639,o.value_numeric,null)) as attempt_number,
                      max(if(o.concept_id=160753,o.value_datetime,null)) as tracing_date,
                      max(if(o.concept_id=164966, o.value_coded ,null)) as tracing_type,
                      max(if(o.concept_id=160721, o.value_coded,null)) as tracing_outcome,
                      max(if(o.concept_id=163725,o.value_coded ,null)) as is_final_trace,
                      max(if(o.concept_id=160433,o.value_coded ,null)) as tracing_outcome_status,
                      max(if(o.concept_id=160716,o.value_text,null)) as voluntary_exit_comment,
                      max(if(o.concept_id=161641, o.value_coded,null)) as status_in_program,
                      max(if(o.concept_id=162568, o.value_coded ,null)) as source_of_information,
                      max(if(o.concept_id=160632, o.value_coded ,null)) as other_informant,
     				 e.date_created as date_last_modified,
                      e.voided as voided
                      from encounter e
                      inner join
                        (
                        select encounter_type_id, uuid, name from encounter_type where uuid in('ce841b19-0acd-46fd-b223-2ca9b5356237')
                        ) et on et.encounter_type_id=e.encounter_type
                      left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                                 and o.concept_id in (165004,165071,1639,160753,164966,160721,163725,160433,160716,161641,162568)
                                     where e.voided=0
               group by e.patient_id, e.encounter_id, visit_date;
               SELECT "Completed processing Peer tracking data ", CONCAT("Time: ", NOW());
               END$$
    -- ------------- populate etl_referral--------------------------------

        DROP PROCEDURE IF EXISTS sp_populate_etl_referral$$
        CREATE PROCEDURE sp_populate_etl_referral()
          BEGIN
            SELECT "Processing referrals ", CONCAT("Time: ", NOW());
            INSERT INTO  kp_etl.etl_referral(
                uuid,
                client_id,
                visit_id,
                visit_date,
                location_id,
                encounter_id,
                encounter_provider,
                date_created,
                referral_order,
                referral_date,
                institution_referred,
                service_referred_for,
                contact_person,
                referred_outcome,
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

                   max(if(o.concept_id=1272,o.value_coded,null)) as referral_order,
                   max(if(o.concept_id=161561,o.value_datetime,null)) as referral_date,
                   max(if(o.concept_id=162724,o.value_text,null)) as institution_referred,
                   max(if(o.concept_id=160478,o.value_coded,null)) as service_referred_for,
                   max(if(o.concept_id=1473,o.value_text,null)) as contact_person,
                    max(if(o.concept_id=165152,o.value_coded,null)) as referred_outcome,
                   max(if(o.concept_id=160632,o.value_text,null)) as remarks,
                   e.voided as voided
                   from encounter e
                   inner join
                     (
                     select encounter_type_id, uuid, name from encounter_type where uuid in('596f878f-5adf-4f8e-8829-6a87aaeda9a3')
                     ) et on et.encounter_type_id=e.encounter_type
                   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                              and o.concept_id in (1272,161561,162724,160478,1473,165152,160632)
                                  where e.voided=0
            group by e.patient_id, e.encounter_id, visit_date;
            SELECT "Completed processing Peer referral data ", CONCAT("Time: ", NOW());
            END$$
      -- ------------- populate etl_alcohol_screening--------------------------------

            DROP PROCEDURE IF EXISTS sp_populate_etl_alcohol_screening$$
            CREATE PROCEDURE sp_populate_etl_alcohol_screening()
              BEGIN
                SELECT "Processing alcohol screening", CONCAT("Time: ", NOW());
                INSERT INTO  kp_etl.etl_alcohol_screening(
                    uuid,
                    client_id,
                    visit_id,
                    visit_date,
                    location_id,
                    encounter_id,
                    encounter_provider,
                    date_created,
                    how_often_do_you_drink,
                    how_many_drinks_you_take,
                    how_often_do_you_drink_six_or_more,
                    how_often_you_cant_stop_once_started,
                    how_often_you_failed_task_due_to_drinking,
                    how_often_you_needed_first_drink,
                    how_often_have_you_felt_guilt,
                    not_able_to_remember_becouse_of_drink,
                    injury_as_a_result_of_drinking,
                    people_concerned_about_your_drinking,
                    total,
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
                       max(if(o.concept_id=159449, o.value_coded ,null)) as how_often_do_you_drink,
                       max(if(o.concept_id=165118, o.value_coded,null)) as how_many_drinks_you_take,
                       max(if(o.concept_id=165119, o.value_coded,null)) as how_often_do_you_drink_six_or_more,
                       max(if(o.concept_id=165120, o.value_coded ,null)) as how_often_you_cant_stop_once_started,
                       max(if(o.concept_id=165121, o.value_coded ,null)) as how_often_you_failed_task_due_to_drinking,
                       max(if(o.concept_id=165122, o.value_coded ,null)) as how_often_you_needed_first_drink,
                       max(if(o.concept_id=165123, o.value_coded ,null)) as how_often_have_you_felt_guilt,
                       max(if(o.concept_id=165124, o.value_coded ,null)) as not_able_to_remember_becouse_of_drink,
                       max(if(o.concept_id=165125, o.value_coded ,null)) as injury_as_a_result_of_drinking,
                       max(if(o.concept_id=165126, o.value_coded ,null)) as people_concerned_about_your_drinking,
                       max(if(o.concept_id=160632,o.value_text,null)) as total,
                       max(if(o.concept_id=163042,o.value_text,null)) as remarks,

                       e.voided as voided
                       from encounter e
                       inner join
                         (
                         select encounter_type_id, uuid, name from encounter_type where uuid in('a3ce2705-d72d-458a-a76c-dae0f93398e7')
                         ) et on et.encounter_type_id=e.encounter_type
                       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                                  and o.concept_id in (159449,165118,165119,165120,165121,165122,165123,165124,165125,165126,160632,163042)
                                      where e.voided=0
                group by e.patient_id, e.encounter_id, visit_date;
                SELECT "Completed processing alcohol screening data ", CONCAT("Time: ", NOW());
                END$$

  -- ------------- populate etl_peer_overdose_reporting--------------------------------

        DROP PROCEDURE IF EXISTS sp_populate_etl_peer_overdose_reporting$$
        CREATE PROCEDURE sp_populate_etl_peer_overdose_reporting()
          BEGIN
            SELECT "Processing peer overdose reporting", CONCAT("Time: ", NOW());
            INSERT INTO  kp_etl.etl_peer_overdose_reporting(
                uuid,
                client_id,
                visit_id,
                visit_date,
                location_id,
                encounter_id,
                encounter_provider,
                date_created,

                address_overdose_happened,
                incident_type,
                hotspot,
                type_of_site,
                naloxone_provided,
                specific_drug_use,
                outcome	,
                reported_by,
                reported_date,
                witnessed_by ,
                witnessed_date ,
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
                   max(if(o.concept_id=162725, o.value_text ,null)) as address_overdose_happened,
                   max(if(o.concept_id=165133, o.value_coded,null)) as incident_type,
                   max(if(o.concept_id=165006, o.value_text,null)) as hotspot,
                   max(if(o.concept_id=165005, o.value_coded ,null)) as type_of_site ,
                   max(if(o.concept_id=165136, o.value_coded ,null)) as naloxone_provided,
                   max(if(o.concept_id=1193, o.value_coded ,null)) as specific_drug_use,
                   max(if(o.concept_id=160632, o.value_text ,null)) as remarks,
                   max(if(o.concept_id=165141, o.value_coded ,null)) as outcome,
                   max(if(o.concept_id=1473, o.value_text ,null)) as reported_by,
                   max(if(o.concept_id=165144, o.value_coded ,null)) as reported_date,
                   max(if(o.concept_id=165143,o.value_text,null)) as witnessed_by,
                   max(if(o.concept_id=160753, o.value_coded ,null)) as witnessed_date,
                   e.voided as voided
                   from encounter e
                   inner join
                     (
                     select encounter_type_id, uuid, name from encounter_type where uuid in('c3fb7831-f8fc-4b71-bd54-f23cdd77e305')
                     ) et on et.encounter_type_id=e.encounter_type
                   left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                              and o.concept_id in (162725,165133,165006,165005,165136,1193,160632,165141,1473,165144,165143,160753)
                                  where e.voided=0
            group by e.patient_id, e.encounter_id, visit_date;
            SELECT "Completed processing peer overdose reporting ", CONCAT("Time: ", NOW());
            END$$
-- ------------- populate etl_CHW_overdose_reporting--------------------------------

            DROP PROCEDURE IF EXISTS sp_populate_etl_CHW_overdose_reporting$$
            CREATE PROCEDURE sp_populate_etl_CHW_overdose_reporting()
              BEGIN
                SELECT "processing CHW overdose reporting dat", CONCAT("Time: ", NOW());
                INSERT INTO  kp_etl.etl_CHW_overdose_reporting(
                    uuid,
                    client_id,
                    visit_id,
                    visit_date,
                    location_id,
                    encounter_id,
                    encounter_provider,
                    date_created,

                    address_overdose_happened,
                    incident_type,
                    hotspot,
                    type_of_site,
                    naloxone_provided,
                    specific_drug_use,
                    outcome	,
                    reported_by,
                    reported_date,
                    witnessed_by ,
                    witnessed_date ,
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
                       max(if(o.concept_id=162725, o.value_text ,null)) as address_overdose_happened,
                       max(if(o.concept_id=165133, o.value_coded,null)) as incident_type,
                       max(if(o.concept_id=165006, o.value_text,null)) as hotspot,
                       max(if(o.concept_id=165005, o.value_coded ,null)) as type_of_site ,
                       max(if(o.concept_id=165136, o.value_coded ,null)) as naloxone_provided,
                       max(if(o.concept_id=1193, o.value_coded ,null)) as specific_drug_use,
                       max(if(o.concept_id=160632, o.value_text ,null)) as remarks,
                       max(if(o.concept_id=165141, o.value_coded ,null)) as outcome,
                       max(if(o.concept_id=1473, o.value_text ,null)) as reported_by,
                       max(if(o.concept_id=165144, o.value_coded ,null)) as reported_date,
                       max(if(o.concept_id=165143,o.value_text,null)) as witnessed_by,
                       max(if(o.concept_id=160753, o.value_coded ,null)) as witnessed_date,
                       e.voided as voided
                       from encounter e
                       inner join
                         (
                         select encounter_type_id, uuid, name from encounter_type where uuid in('383974fe-58ef-488f-bdff-8962f4dd7518')
                         ) et on et.encounter_type_id=e.encounter_type
                       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                                  and o.concept_id in (162725,165133,165006,165005,165136,1193,160632,165141,1473,165144,165143,160753)
                                      where e.voided=0
                group by e.patient_id, e.encounter_id, visit_date;
                SELECT "Completed processing CHW overdose reporting data ", CONCAT("Time: ", NOW());
                END$$

            -- ------------- populate etl_patient_triage--------------------------------
                DROP PROCEDURE IF EXISTS sp_populate_etl_patient_triage$$
                CREATE PROCEDURE sp_populate_etl_patient_triage()
                    BEGIN
                        SELECT "Processing Patient Triage ", CONCAT("Time: ", NOW());
                        INSERT INTO kp_etl.etl_patient_triage(
                          uuid,
                          client_id,
                          visit_id,
                          visit_date,
                          location_id,
                          encounter_id,
                          encounter_provider,
                          date_created,
                            visit_reason,
                            weight,
                            height,
                            systolic_pressure,
                            diastolic_pressure,
                            temperature,
                            pulse_rate,
                            respiratory_rate,
                            oxygen_saturation,
                            muac,
                            nutritional_status,
                            last_menstrual_period,
                      date_last_modified,
                            voided
                        )
                            select
                                e.uuid,
                                e.patient_id,
                                e.visit_id,
                                date(e.encounter_datetime) as visit_date,
                                e.location_id,
                                e.encounter_id as encounter_id,
                                e.creator,
                                e.date_created as date_created,
                                max(if(o.concept_id=160430,trim(o.value_text),null)) as visit_reason,
                                max(if(o.concept_id=5089,o.value_numeric,null)) as weight,
                                max(if(o.concept_id=5090,o.value_numeric,null)) as height,
                                max(if(o.concept_id=5085,o.value_numeric,null)) as systolic_pressure,
                                max(if(o.concept_id=5086,o.value_numeric,null)) as diastolic_pressure,
                                max(if(o.concept_id=5088,o.value_numeric,null)) as temperature,
                                max(if(o.concept_id=5087,o.value_numeric,null)) as pulse_rate,
                                max(if(o.concept_id=5242,o.value_numeric,null)) as respiratory_rate,
                                max(if(o.concept_id=5092,o.value_numeric,null)) as oxygen_saturation,
                                max(if(o.concept_id=1343,o.value_numeric,null)) as muac,
                                max(if(o.concept_id=163300,o.value_coded,null)) as nutritional_status,
                                max(if(o.concept_id=1427,date(o.value_datetime),null)) as last_menstrual_period,
                                if(max(o.date_created)!=min(o.date_created),max(o.date_created),NULL) as date_last_modified,
                                e.voided as voided
                            from encounter e
                                inner join person p on p.person_id=e.patient_id and p.voided=0
                                inner join
                                (
                                    select encounter_type_id, uuid, name from encounter_type where uuid in('55e67467-bd0b-4940-82c2-3281938afde3')
                                ) et on et.encounter_type_id=e.encounter_type
                                left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                and o.concept_id in (160430,5089,5090,5085,5086,5088,5087,5242,5092,1343,163300,1427)
                            where e.voided=0
                            group by e.patient_id, e.encounter_id, visit_date
                        ;
                        SELECT "Completed processing Patient Triage data ", CONCAT("Time: ", NOW());
                        END$$

          -- ------------- populate etl_diagnosis--------------------------------

            DROP PROCEDURE IF EXISTS sp_populate_etl_diagnosis$$
            CREATE PROCEDURE sp_populate_etl_diagnosis()
              BEGIN
                SELECT "Processing diagnosis", CONCAT("Time: ", NOW());
                INSERT INTO  kp_etl.etl_diagnosis(
                    uuid,
                    client_id,
                    visit_id,
                    visit_date,
                    location_id,
                    encounter_id,
                    encounter_provider,
                    date_created,
                    skin_findings,
                    skin_finding_notes,
                    eyes_findings,
                    eyes_finding_notes,
                    ent_findings,
                    ent_finding_notes,
                    chest_findings,
                    chest_finding_notes,
                    cvs_findings,
                    cvs_finding_notes,
                    abdomen_findings,
                    abdomen_finding_notes,
                    cns_findings,
                    cns_finding_notes,
                    genitourinary_findings,
                    genitourinary_finding_notes,
                    diagnosis,
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
                       max(if(o.concept_id=1120, o.value_coded ,null)) as skin_findings,
                       max(if(o.concept_id=160981, o.value_text,null)) as skin_finding_notes,
                       max(if(o.concept_id=163309, o.value_coded ,null)) as eyes_findings ,
                       max(if(o.concept_id=164938, o.value_text,null)) as eyes_finding_notes,
                       max(if(o.concept_id=164936, o.value_coded ,null)) as ent_findings,
                       max(if(o.concept_id=164939, o.value_text ,null)) as ent_finding_notes,
                       max(if(o.concept_id=1123, o.value_coded ,null)) as chest_findings,
                       max(if(o.concept_id=163042,o.value_text,null)) as chest_finding_notes,
                       max(if(o.concept_id=1124, o.value_coded ,null)) as cvs_findings,
                       max(if(o.concept_id=163046, o.value_text ,null)) as cvs_finding_notes,
                       max(if(o.concept_id=1125, o.value_coded ,null)) as abdomen_findings,
                       max(if(o.concept_id=160948,o.value_text,null)) as abdomen_finding_notes,
                       max(if(o.concept_id=164937, o.value_coded ,null)) as cns_findings,
                       max(if(o.concept_id=160994, o.value_text ,null)) as cns_finding_notes,
                       max(if(o.concept_id=1126, o.value_coded ,null)) as genitourinary_findings,
                       max(if(o.concept_id=163047,o.value_text,null)) as genitourinary_finding_notes,
                       max(if(o.concept_id=159947,o.value_text,null)) as diagnosis,
                       max(if(o.concept_id=162169,o.value_text,null)) as clinical_notes,
                       e.voided as voided
                       from encounter e
                       inner join
                         (
                         select encounter_type_id, uuid, name from encounter_type where uuid in('928ea6b2-3425-4ee9-854d-daa5ceaade03')
                         ) et on et.encounter_type_id=e.encounter_type
                       left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                                  and o.concept_id in (1120,160981,163309,164938,164936,164939,1123,163042,1124,163046,1125,160948,164937,160994,1126,163047,159947,162169)
                                      where e.voided=0
                group by e.patient_id, e.encounter_id, visit_date;
                SELECT "Completed processing diagnosis data ", CONCAT("Time: ", NOW());
                END$$





SET sql_mode=@OLD_SQL_MODE$$
-- ------------------------------------------- running all procedures -----------------------------

DROP PROCEDURE IF EXISTS sp_first_time_setup$$
CREATE PROCEDURE sp_first_time_setup()
  BEGIN
DECLARE populate_script_id INT(11);
SELECT "Beginning first time setup", CONCAT("Time: ", NOW());
INSERT INTO kp_etl.etl_script_status(script_name, start_time) VALUES('initial_population_of_tables', NOW());
SET populate_script_id = LAST_INSERT_ID();

CALL sp_populate_etl_client_registration();
CALL sp_populate_etl_contact();
CALL sp_populate_etl_client_enrollment();
CALL sp_populate_etl_clinical_visit();
CALL sp_populate_etl_sti_treatment();
CALL sp_populate_etl_peer_calendar();
CALL sp_populate_hts_test();
CALL sp_populate_violence_screening();
CALL sp_populate_etl_peer_tracking();
CALL sp_populate_etl_referral();
CALL sp_populate_etl_alcohol_screening();
CALL sp_populate_etl_peer_overdose_reporting();
CALL sp_populate_etl_CHW_overdose_reporting();
CALL sp_populate_etl_patient_triage();
CALL sp_populate_etl_diagnosis();

UPDATE kp_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed first time setup", CONCAT("Time: ", NOW());

END$$




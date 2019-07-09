
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
            avg_weekly_drug_injections,
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
        from encounter e
               inner join
                 (
                 select encounter_type_id, uuid, name from encounter_type where uuid='ea68aad6-4655-4dc5-80f2-780e33055a9e'
                 ) et on et.encounter_type_id=e.encounter_type
               join patient p on p.patient_id=e.patient_id and p.voided=0
               left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
                                          and o.concept_id in (164929,165004,165137,165006,165005,165030,165031,165032,165007,165008,165009,160638,165038,160642)
        where e.voided=0
        group by e.patient_id, e.encounter_id;

        SELECT "Completed processing KP contact data", CONCAT("Time: ", NOW());

        update kp_etl.etl_contact c
        join (select pi.patient_id,
                     max(if(pit.uuid='b7bfefd0-239b-11e9-ab14-d663bd873d93',pi.identifier,null)) unique_identifier
              from patient_identifier pi
                     join patient_identifier_type pit on pi.identifier_type=pit.patient_identifier_type_id
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
                    condom_use_education,
                    post_abortal_care,
                    linked_to_psychosocial,
                    male_condoms_no,
                    female_condoms_no,
                    lubes_no,
                    syringes_needles_no,
                    pep,
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
                       max(if(o.concept_id=161558,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as sti_screened,
                       max(if(o.concept_id=165199,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as sti_results,
                       max(if(o.concept_id=165200,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as sti_treated,
                       max(if(o.concept_id=165249,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as sti_referred,
                       max(if(o.concept_id=165250,o.value_text,null)) as sti_referred_text,
                       max(if(o.concept_id=165197,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as tb_screened,
                       max(if(o.concept_id=165198,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as tb_results,
                       max(if(o.concept_id=1111,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as tb_treated,
                       max(if(o.concept_id=162310,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as tb_referred,
                       max(if(o.concept_id=163323,o.value_text,null)) as tb_referred_text,
                       max(if(o.concept_id=165040,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as hepatitisB_screened,
                       max(if(o.concept_id=1322,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as hepatitisB_results,
                       max(if(o.concept_id=165251,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as hepatitisB_treated,
                       max(if(o.concept_id=165252,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as hepatitisB_referred,
                       max(if(o.concept_id=165253,o.value_text,null)) as hepatitisB_text,
                       max(if(o.concept_id=165041,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as hepatitisC_screened,
                       max(if(o.concept_id=161471,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" else "" end),null)) as hepatitisC_results,
                       max(if(o.concept_id=165254,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as hepatitisC_treated,
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
                       max(if(o.concept_id=165043,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as alcohol_screened,
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
                       max(if(o.concept_id=165203,(case o.value_coded when 1065 then "Initiated" when 1066 THEN "Not Initiated" else "" end),null)) as prep_treated,
                       max(if(o.concept_id=165270,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as prep_referred,
                       max(if(o.concept_id=165271,o.value_text,null)) as prep_text,
                       max(if(o.concept_id=165204,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as violence_screened,
                       max(if(o.concept_id=165205,(case o.value_coded when 165206 then "Harrasment" when 165207 THEN "Illegal arrest" when 123007 THEN "Verbal Abuse" when 127910 THEN "Rape/Sexual assault" when 126312 THEN "Discrimination"  else "" end),null)) as violence_results,
                       max(if(o.concept_id=165208,(case o.value_coded when  1065 then "Supported" when 1066 THEN "Not supported" else "" end),null)) as violence_treated,
                       max(if(o.concept_id=165273,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as violence_referred,
                       max(if(o.concept_id=165274,o.value_text,null)) as violence_text,
                       max(if(o.concept_id=165045,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as risk_red_counselling_screened,
                       max(if(o.concept_id=165050,(case o.value_coded when 165087 then "Eligible" when 165078 THEN "Not eligible" else "" end),null)) as risk_red_counselling_eligibility,
                       max(if(o.concept_id=165053,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as risk_red_counselling_support,
                       max(if(o.concept_id=161595,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as risk_red_counselling_ebi_provided,
                       max(if(o.concept_id=165277,o.value_text,null)) as risk_red_counselling_text,
                       max(if(o.concept_id=1382,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as fp_screened,
                       max(if(o.concept_id=165209,(case o.value_coded when 165087 then "Eligible" when 165078 THEN "Not eligible" else "" end),null)) as fp_eligibility,
                       max(if(o.concept_id=160653,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as fp_treated,
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
                       max(if(o.concept_id=159427,(case o.value_coded when 664 then "Negative" when 703 THEN "Positive" when 1118 then "Not done" else "" end),null)) as test_results,
                       max(if(o.concept_id=1648,(case o.value_coded when 1 then "Yes" when 0 then "No" else "" end),null)) as linked_to_art,
                       max(if(o.concept_id=163042,o.value_text,null)) as facility_linked_to,
                       max(if(o.concept_id=165220,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as self_test_education,
                       max(if(o.concept_id=165221,(case o.value_coded when 165222 then "Self use" when 165223 THEN "Distribution" else "" end),null)) as self_test_kits_given,
                       max(if(o.concept_id=165222,o.value_text,null)) as self_use_kits,
                       max(if(o.concept_id=165223,o.value_text,null)) as distribution_kits,
                       max(if(o.concept_id=164952,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as self_tested,
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
                       max(if(o.concept_id=165243,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" when 1175 then "Not Applicable" else "" end),null)) as vl_test_done,
                       max(if(o.concept_id=165246,(case o.value_coded when 165244 THEN "Suppressed" when 165245 then "Not suppressed" when 164369 then "Results not yet received" when 1175 then "Not Applicable" else "" end),null)) as vl_results,
                       max(if(o.concept_id=165247,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as condom_use_education,
                       max(if(o.concept_id=164820,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as post_abortal_care,
                       max(if(o.concept_id=165302,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as linked_to_psychosocial,
                       max(if(o.concept_id=165055,o.value_numeric,null)) as male_condoms_no,
                       max(if(o.concept_id=165056,o.value_numeric,null)) as female_condoms_no,
                       max(if(o.concept_id=165057,o.value_numeric,null)) as lubes_no,
                       max(if(o.concept_id=165058,o.value_numeric,null)) as syringes_needles_no,
                       max(if(o.concept_id=164845,(case o.value_coded when 127910 THEN "Rape" when 165045 then "Condom burst" when 5622 then "Others" else "" end),null)) as pep,
                       max(if(o.concept_id=165060,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as exposure_type,
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
                                                  and o.concept_id in (161558,165199,165200,165249,165250,165197,165198,1111,162310,163323,165040,1322,165251,165252,165253,
                        165041,161471,165254,165255,165256,165042,165046,165257,165201,165258,165259,165044,165051,165260,165261,165262,165043,165047,165263,165264,165265,
                        164934,165196,165266,165267,165268,165076,165202,165203,165270,165271,165204,165205,165208,165273,165274,165045,165050,165053,161595,165277,1382,
                        165209,160653,165279,165280,165210,165211,165213,165281,165282,165214,165215,159382,164401,165218,164848,159427,1648,163042,165220,165221,165222,165223,
                        164952,164400,165231,165233,165234,165237,162724,165238,161562,165239,163042,165240,160119,165242,165243,165246,165247,164820,165302,165055,165056,
                        165057,165058,164845,165248,5096)
                where e.voided=0
                group by e.patient_id, e.encounter_id, visit_date;
                SELECT "Completed processing Clinical visit data ", CONCAT("Time: ", NOW());
                END$$


-- ------------- populate etl_triage--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_triage$$
CREATE PROCEDURE sp_populate_etl_triage()
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

DROP PROCEDURE IF EXISTS sp_populate_etl_pregnancy_fp_cacx_screening$$
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
 max(if(o.concept_id=165087,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as eligible_for_fp,
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
 max(if(o.concept_id=165086,(case o.value_coded when 1065 then "Yes" when 1066 THEN "No" else "" end),null)) as cacx_screening,
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
   and o.concept_id in (1427,5272,5596,160653,165087,374,165082,164934,165086,165052,1272)
where e.voided=0
group by e.patient_id, e.encounter_id, visit_date;
SELECT "Completed processing pregnancy, family planning and CaCz screening data ", CONCAT("Time: ", NOW());
END$$


-- ------------- populate sp_populate_etl_adverse_drug_reaction--------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_adverse_drug_reaction$$
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

  DROP PROCEDURE IF EXISTS sp_populate_etl_sti_screening$$
  CREATE PROCEDURE sp_populate_etl_sti_screening()
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

DROP PROCEDURE IF EXISTS sp_populate_etl_hepatitis_screening$$
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
max(if(o.concept_id=164082,(case o.value_coded when 165040 then "Hepatitis B" when 165041 THEN "Hepatitis C" else "" end),null)) as hepatitis_screening_done,
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


-- ------------- populate etl_tb_screening-------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_tb_screening$$
CREATE PROCEDURE sp_populate_etl_tb_screening()
  BEGIN
 SELECT "Processing TB Screening data ", CONCAT("Time: ", NOW());

 insert into kp_etl.etl_tb_screening(
uuid,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
encounter_provider,
date_created,
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
 max(if(o.concept_id=1659,(case o.value_coded when 1660 then "No TB Signs" when 142177 THEN "Presumed TB" when 1662 then "TB Confirmed" when 160737 then "TB Screening Not Done" else "" end),null)) as tb_results_status,
 max(if(o.concept_id=162309,(case o.value_coded when 1065 THEN "Yes" when 1066 then "No" else "" end),null)) as start_anti_TB,
 max(if(o.concept_id=1113,o.value_datetime,null)) as tb_treatment_date,
 max(if(o.concept_id=1111,o.value_coded,null)) as tb_treatment,
 e.voided as voided
 from encounter e
 inner join form f on f.form_id=e.form_id and f.uuid in ('22c68f86-bbf0-49ba-b2d1-23fa7ccf0259','59ed8e62-7f1f-40ae-a2e3-eabe350277ce')
 inner join obs o on o.encounter_id = e.encounter_id
 where e.voided=0
 group by e.encounter_id;

 SELECT "Completed processing TB Screening data ", CONCAT("Time: ", NOW());
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
 max(if(o.concept_id=164082,(case o.value_coded when 165043 then "Alcohol" when 165044 then "Abscess" when 165045 then "Risk" when 165042 then "Drugs" else "" end),null)) as screened_for,
 max(if(o.concept_id=165047,(case o.value_coded
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
  and o.concept_id in (164082,165047,165038,1272,160632)
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



-- ------------- populate etl_counselling_services-------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_counselling_services$$
CREATE PROCEDURE sp_populate_etl_counselling_services()
BEGIN
  SELECT "Processing counselling services", CONCAT("Time: ", NOW());
  INSERT INTO kp_etl.etl_counselling_services(
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
 max(if(o.concept_id=165070,(case o.value_coded
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
   and o.concept_id in (165070,1272,160632)
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
 when 165076 then "PrEP"
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
 max(if(o.concept_id=165060,(case o.value_coded
 when 127910 then "Rape"
 when 165059 then "Condom burst"
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
  and o.concept_id in (164082,165028,1272,164845,165060,160632 )
  where e.voided=0
  group by e.patient_id, e.encounter_id, visit_date;
  SELECT "Completed PrEp/PEp screening data ", CONCAT("Time: ", NOW());

  END$$


  -- ------------------------------------ populate etl_hts_test table ----------------------------------------

  DROP PROCEDURE IF EXISTS sp_populate_etl_hts_test$$
  CREATE PROCEDURE sp_populate_etl_hts_test()
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
  max(if(o.concept_id=162502,date(o.value_datetime),null)) as expiry_date
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
   max(if(o.concept_id=160555,o.value_datetime,null)) as enrollment_date,
   max(if(o.concept_id=159599,o.value_datetime,null)) as art_start_date,
   max(if(o.concept_id=162053,o.value_numeric,null)) as ccc_number,
   max(if(o.concept_id=1473,trim(o.value_text),null)) as provider_handed_to,
   max(if(o.concept_id=162577,(case o.value_coded when 1577 then "Nurse"
  when 1574 then "Clinical Officer/Doctor"
  when 1555 then "Community Health Worker"
  when 1540 then "Employee"
  when 5622 then "Other" else "" end),null)) as provider_cadre,
   max(if(o.concept_id=163042,trim(o.value_text),null)),
   e.voided
from encounter e
   inner join form f on f.form_id = e.form_id and f.uuid = "050a7f12-5c52-4cad-8834-863695af335d"
   left outer join obs o on o.encounter_id = e.encounter_id and o.concept_id in (164966, 159811, 162724, 160555, 159599, 162053, 1473, 162577, 163042) and o.voided=0
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
   max(if(o.concept_id=164966,(case o.value_coded when 1650 then "Phone" when 164965 then "Physical" else "" end),null)) as tracing_type ,
   max(if(o.concept_id=160721,(case o.value_coded when 1065 then "Reached" when 1066 then "Not Reached" else "" end),null)) as tracing_outcome,
   max(if(o.concept_id=165071,(case o.value_coded
 when 165074 then "Inaccurate contact details"
 when 165075 then "Inaccurate location details"
 when 165072 then "Missing contact details"
 when 165073 then "Missing location details"
 when 5622 then "Other"
 else "" end),null)) as negative_outcome_reason,
   max(if(o.concept_id=163042,trim(o.value_text),null)) as negative_outcome_description,
   max(if(o.concept_id=162502,o.value_datetime,null)) as next_tracing_attempt_date,
   max(if(o.concept_id=165068,(case o.value_coded
 when 165062 then "Self Transfer"
 when 164349 then "Treatment interrupted/stopped"
 when 165063 then "Willing to return to services"
 when 160415 then "Moved away"
 when 165064 then "Moved to another hotspot"
 when 165065 then "Not happy with the program"
 when 165066 then "Stopped sex work and/or injecting drugs"
 when 162277 then "In prison"
 when 160432 then "Dead"
 when 165067 then "Untraceable"
 else "" end),null)) as final_tracing_status,
   e.voided
from encounter e
   inner join form f on f.form_id = e.form_id and f.uuid = "050a7f12-5c52-4cad-8834-863695af335d"
   left outer join obs o on o.encounter_id = e.encounter_id and o.concept_id in (162502,164966,160721, 165071,163042, 162502, 165068) and o.voided=0
group by e.encounter_id;
SELECT "Completed processing Client tracing";
END$$


-- ------------------------------------ Populate HIV status -------------------------------

DROP PROCEDURE IF EXISTS sp_populate_etl_hiv_status$$
CREATE PROCEDURE sp_populate_etl_hiv_status()
  BEGIN
SELECT "Processing hiv status";

INSERT INTO kp_etl.etl_hiv_status(
uuid ,
provider,
client_id,
visit_id,
visit_date,
location_id,
encounter_id,
date_created,
ever_tested,
test_date,
test_results_status,
current_in_care,
referral,
art_start_date,
treatment_facility,
current_regimen,
recent_vl_result,
vl_test_date,
provider_referred_to,
voided

)
select
   e.uuid,
   e.creator,
   e.patient_id,
   e.visit_id,
   e.encounter_datetime as visit_date,
   e.location_id,
   e.encounter_id,
   e.date_created,
   max(if(o.concept_id=164401,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null))as ever_tested,
   max(if(o.concept_id=164400,o.value_datetime,null)) as test_date,
   max(if(o.concept_id=159427,(case o.value_coded when 703 then "Positive" when 664 then "Negative" else "" end),null)) as test_results_status,
   max(if(o.concept_id=159811,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as current_in_care,
   max(if(o.concept_id=164849,(case o.value_coded when 1065 then "Yes" when 1066 then "No" else "" end),null)) as referral,
   max(if(o.concept_id=159599,o.value_datetime,null)) as art_start_date,
   max(if(o.concept_id=162724,o.value_text,null)) as treatment_facility,
   max(if(o.concept_id=162240,(case o.value_coded
   when  84795 then  "TENOFOVIR"
   when  164510 then  "TDF-ddl-NFV"
   when  164509 then  "TDF-ddl-LPV /R"
   when  104567 then  "EMTRICITABINE / TENOFOVIR DISOPROXIL"
   when  162565 then  "Lamivudine / Nevirapine / Tenofovir"
   when  164512 then  "TDF-3TC-ATV/r"
   when  104565 then  "EFAVIRENZ / EMTRICITABINE / TENOFOVIR DISOPROXIL"
   when  162201 then  "3TC, LPV/r, TDF"
   when  164854 then  "Tenofovir / Emtricitabine / Nevirapine"
   when  164971 then  "Tenofovir/Lamivudine/Zidovudine"
   when  162562 then  "Abacavir / Lopinavir / Ritonavir / Tenofovir"
   when  164974 then  "Etravirine/Tenofovir/Lamivudine/Lopinavir"
   when  164972 then  "Zidovudine/Tenofovir/Lamivudine/Lopinavir"
   when  161364 then  "lamivudine / tenofovir"
   when  164969 then  "Tenofovir / Lamivudine / Dolutegravir"
   when  164976 then  "Abacavir/Tenofovir/Lamivudine/Lopinavir"
  else "" end),null)) as current_regimen,
   max(if(o.concept_id=163042,o.value_text,null)) as recent_vl_result,
   max(if(o.concept_id=162502,o.value_datetime,null)) as vl_test_date,
   max(if(o.concept_id=1473,o.value_text,null)) as provider_referred_to,
   e.voided as voided

from encounter e
inner join
(
select encounter_type_id, uuid, name from encounter_type where uuid in('5710da76-a36f-4c44-a67f-f76135b280dc')
) et on et.encounter_type_id=e.encounter_type
left outer join obs o on o.encounter_id=e.encounter_id and o.voided=0
  and o.concept_id in (164401,164400,159427,159811,164849,159599,162724,162240,163042,1473,162502)
  and o.voided=0

group by e.encounter_id;
SELECT "Completed processing HIV status";

  END$$
SET sql_mode=@OLD_SQL_MODE$$
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
CALL sp_populate_etl_triage();
CALL sp_populate_etl_client_complaints();
CALL sp_populate_etl_chronic_illness();
CALL sp_populate_etl_allergies();
CALL sp_populate_etl_pregnancy_fp_cacx_screening();
CALL sp_populate_etl_adverse_drug_reaction();
CALL sp_populate_etl_immunization_screening();
CALL sp_populate_etl_sti_screening();
CALL sp_populate_etl_hepatitis_screening();
CALL sp_populate_etl_tb_screening();
CALL sp_populate_etl_systems_review();
CALL sp_populate_etl_diagnosis_treatment();
CALL sp_populate_etl_clinical_notes();
CALL sp_populate_etl_appointment();
CALL sp_populate_etl_alcohol_drugs_risk_screening();
CALL sp_populate_etl_violence_screening();
CALL sp_populate_etl_counselling_services();
CALL sp_populate_etl_prep_pep_screening();
CALL sp_populate_etl_hts_test();
CALL sp_populate_etl_hts_referral_and_linkage();
CALL sp_populate_etl_client_tracing();
CALL sp_populate_etl_hiv_status();

UPDATE kp_etl.etl_script_status SET stop_time=NOW() where id= populate_script_id;

SELECT "Completed first time setup", CONCAT("Time: ", NOW());
  
END$$




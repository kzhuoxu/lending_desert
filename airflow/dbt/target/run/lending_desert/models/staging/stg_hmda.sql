

  create or replace view `fit-reference-447221-v2`.`lending_desert`.`stg_hmda`
  OPTIONS()
  as -- Filter HMDA to target MSAs, select columns relevant to lending desert analysis
WITH target_msas AS (
    SELECT DISTINCT msa_md FROM `fit-reference-447221-v2`.`lending_desert`.`msa_lookup`
)

SELECT
    CAST(activity_year AS INT64) AS activity_year,
    CAST(census_tract AS STRING) AS census_tract,
    CAST(derived_msa_md AS INT64) AS msa_md,
    CAST(action_taken AS INT64) AS action_taken,
    denial_reason_1,
    derived_race,
    derived_ethnicity,
    CAST(loan_amount AS FLOAT64) AS loan_amount,
    loan_purpose,
    loan_type,
    -- Classify race group for analysis
    CASE
        WHEN derived_race = 'White' AND derived_ethnicity = 'Not Hispanic or Latino' THEN 'White Non-Hispanic'
        ELSE 'Minority'
    END AS race_group,
    -- Flag collateral/appraisal denials
    CASE
        WHEN CAST(action_taken AS INT64) = 3
         AND denial_reason_1 = '4' THEN TRUE
        ELSE FALSE
    END AS is_collateral_denial

FROM `fit-reference-447221-v2`.`lending_desert`.`hmda_lar_raw`
WHERE CAST(derived_msa_md AS INT64) IN (SELECT msa_md FROM target_msas)
  AND CAST(action_taken AS INT64) IN (1, 3)  -- originated or denied only
  AND loan_purpose = '1'  -- home purchase only;


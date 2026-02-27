
  
    

    create or replace table `fit-reference-447221-v2`.`lending_desert`.`mrt_opportunity_score`
      
    
    

    OPTIONS()
    as (
      -- Join tract denial metrics with ACS demographics, compute opportunity score
WITH scored AS (
    SELECT
        d.census_tract,
        d.msa_md,
        d.total_applications,
        d.denial_rate,
        d.collateral_denial_pct,
        d.minority_denials,
        d.white_denials,
        d.minority_collateral_denials,
        d.white_collateral_denials,
        d.dti_denials,
        d.credit_history_denials,
        d.collateral_denials,
        d.employment_denials,
        d.insufficient_cash_denials,
        a.total_population,
        a.median_household_income,
        a.median_home_value,
        a.minority_pct,
        a.state_abbr,
        a.tract_name,

        -- Percentile ranks for scoring components
        PERCENT_RANK() OVER (ORDER BY d.denial_rate) AS denial_rate_rank,
        PERCENT_RANK() OVER (ORDER BY d.collateral_denial_pct) AS collateral_rank,
        PERCENT_RANK() OVER (ORDER BY LOG(d.total_applications + 1)) AS volume_rank,
        PERCENT_RANK() OVER (ORDER BY a.minority_pct) AS minority_rank

    FROM `fit-reference-447221-v2`.`lending_desert`.`mrt_tract_denials` d
    JOIN `fit-reference-447221-v2`.`lending_desert`.`stg_acs` a ON d.census_tract = a.census_tract
)

SELECT
    *,
    ROUND(
        0.30 * denial_rate_rank
      + 0.30 * collateral_rank
      + 0.20 * volume_rank
      + 0.20 * minority_rank,
    3) AS opportunity_score

FROM scored
    );
  
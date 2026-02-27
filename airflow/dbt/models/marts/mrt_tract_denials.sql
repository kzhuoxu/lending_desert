-- Per-tract denial metrics and denial reason breakdown by race group
SELECT
    census_tract,
    msa_md,

    -- Volume
    COUNT(*) AS total_applications,
    COUNTIF(action_taken = 3) AS total_denials,
    COUNTIF(action_taken = 1) AS total_originated,

    -- Denial rate
    SAFE_DIVIDE(COUNTIF(action_taken = 3), COUNT(*)) AS denial_rate,

    -- Collateral denial share (among denials)
    SAFE_DIVIDE(
        COUNTIF(is_collateral_denial),
        COUNTIF(action_taken = 3)
    ) AS collateral_denial_pct,

    -- Denial reasons by race group (for bar chart)
    COUNTIF(action_taken = 3 AND race_group = 'Minority') AS minority_denials,
    COUNTIF(action_taken = 3 AND race_group = 'White Non-Hispanic') AS white_denials,
    COUNTIF(is_collateral_denial AND race_group = 'Minority') AS minority_collateral_denials,
    COUNTIF(is_collateral_denial AND race_group = 'White Non-Hispanic') AS white_collateral_denials,

    -- Denial reason counts (for breakdown chart)
    COUNTIF(action_taken = 3 AND denial_reason_1 = '1') AS dti_denials,
    COUNTIF(action_taken = 3 AND denial_reason_1 = '3') AS credit_history_denials,
    COUNTIF(action_taken = 3 AND denial_reason_1 = '4') AS collateral_denials,
    COUNTIF(action_taken = 3 AND denial_reason_1 = '2') AS employment_denials,
    COUNTIF(action_taken = 3 AND denial_reason_1 = '5') AS insufficient_cash_denials

FROM {{ ref('stg_hmda') }}
GROUP BY census_tract, msa_md
HAVING COUNT(*) >= 10  -- minimum application threshold

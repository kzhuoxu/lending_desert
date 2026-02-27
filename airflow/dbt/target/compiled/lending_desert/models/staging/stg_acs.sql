-- Clean ACS tract data and compute minority percentage
SELECT
    CAST(census_tract AS STRING) AS census_tract,
    state_abbr,
    NAME AS tract_name,
    CAST(B01003_001E AS INT64) AS total_population,
    CAST(B19013_001E AS INT64) AS median_household_income,
    CAST(B25077_001E AS INT64) AS median_home_value,
    CAST(B02001_002E AS INT64) AS white_alone,
    CAST(B02001_003E AS INT64) AS black_alone,
    CAST(B03003_003E AS INT64) AS hispanic_latino,
    -- Minority percentage
    SAFE_DIVIDE(
        CAST(B02001_001E AS INT64) - CAST(B02001_002E AS INT64),
        CAST(B02001_001E AS INT64)
    ) AS minority_pct

FROM `fit-reference-447221-v2`.`lending_desert`.`acs_tract_raw`
WHERE CAST(B01003_001E AS INT64) > 0  -- exclude empty tracts
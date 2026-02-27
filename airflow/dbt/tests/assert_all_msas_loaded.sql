-- Singular test: every MD code in msa_lookup must have at least one tract in
-- mrt_tract_denials. Fails (returns rows) if any expected MD is missing,
-- which indicates an incomplete HMDA ingestion for that metro division.
SELECT msa_md, short_name
FROM {{ ref('msa_lookup') }}
WHERE msa_md NOT IN (
    SELECT DISTINCT msa_md FROM {{ ref('mrt_tract_denials') }}
)
GROUP BY msa_md, short_name

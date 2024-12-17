WITH last_non_direct_click_table AS (
  SELECT
    user_pseudo_id,
    transaction_id,
    CASE
      WHEN session_source_medium != '(none) / (direct)' THEN session_source_medium
      WHEN session_number > 1 AND session_source_medium = '(none) / (direct)' THEN LAG(session_source_medium) OVER (
        PARTITION BY user_pseudo_id
        ORDER BY session_number
      )
      ELSE '(none) / (direct)'
    END AS source_medium,
    1 AS attribution_weight
  FROM
    `projekt-testowy-428607.attribution_modeling.combined_table`
  WHERE
    transaction_id IS NOT NULL
)
SELECT
  source_medium,
  SUM(attribution_weight) AS last_non_direct_click
FROM
  last_non_direct_click_table
GROUP BY
  source_medium;

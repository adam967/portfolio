WITH linear_table AS (
  SELECT
    user_pseudo_id,
    session_source_medium AS source_medium,
    LAST_VALUE(transaction_id) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY transaction_timestamp ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS transaction,
    1 / (
      SELECT MAX(session_number)
      FROM `projekt-testowy-428607.attribution_modeling.combined_table`
      WHERE transaction_id IS NOT NULL AND ct.user_pseudo_id = user_pseudo_id
    ) AS attribution_weight
  FROM
    `projekt-testowy-428607.attribution_modeling.combined_table` AS ct
)
SELECT
  source_medium,
  CAST(ROUND(SUM(attribution_weight), 0) AS INTEGER) AS linear_model
FROM
  linear_table
GROUP BY
  source_medium;

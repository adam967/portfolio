WITH time_decay_table AS (
  SELECT
    user_pseudo_id,
    session_source_medium AS source_medium,
    LAST_VALUE(transaction_id) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY transaction_timestamp ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS transaction,
    CASE
      WHEN total_sessions = 1 THEN 1
      ELSE SAFE_DIVIDE(
        POWER(2, session_number / total_sessions),
        SUM(POWER(2, session_number / total_sessions)) OVER(PARTITION BY user_pseudo_id)
      )
    END AS attribution_weight
  FROM
    `projekt-testowy-428607.attribution_modeling.combined_table`
)
SELECT
  source_medium,
  CAST(ROUND(SUM(attribution_weight), 0) AS INTEGER) AS time_decay_click
FROM
  time_decay_table
GROUP BY
  source_medium;

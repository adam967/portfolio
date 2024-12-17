WITH u_shape_table AS (
  SELECT
    user_pseudo_id,
    session_source_medium AS source_medium,
    LAST_VALUE(transaction_id) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY transaction_timestamp ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS transaction,
    CASE
      WHEN total_sessions = 1 THEN 1
      WHEN total_sessions = 2 THEN 0.5
      WHEN total_sessions > 2 THEN (
        CASE
          WHEN session_number = 1 THEN 0.4
          WHEN session_number = total_sessions THEN 0.4
          ELSE 0.2 / (total_sessions - 2)
        END
      )
    END AS attribution_weight
  FROM
    `projekt-testowy-428607.attribution_modeling.combined_table` AS ct
)
SELECT
  source_medium,
  CAST(ROUND(SUM(attribution_weight), 0) AS INTEGER) AS u_shape_click
FROM
  u_shape_table
GROUP BY
  source_medium;

-- Declaring a variable with the number of days of the attribution window
DECLARE attribution_window INT64 DEFAULT 30;

-- Table with transactions
WITH transactions AS (
  SELECT
    ecommerce.transaction_id AS transaction_id,
    MAX(event_timestamp) AS transaction_timestamp,
    user_pseudo_id AS user_pseudo_id,
    event_params.value.int_value AS session_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    CROSS JOIN UNNEST(event_params) AS event_params
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
    AND event_name = 'purchase'
    AND ecommerce.transaction_id IS NOT NULL
    AND event_params.key = 'ga_session_id'
  GROUP BY
    transaction_id, user_pseudo_id, session_id
),

-- Table with sessions
sessions AS (
  SELECT
    event_params.value.int_value AS session_id,
    MAX(event_timestamp) AS session_start_timestamp,
    user_pseudo_id AS user_pseudo_id,
    CONCAT(traffic_source.source, ' / ', traffic_source.medium) AS session_source_medium
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    CROSS JOIN UNNEST(event_params) AS event_params
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
    AND event_name = 'session_start'
    AND event_params.key = 'ga_session_id'
    AND user_pseudo_id IN (
      SELECT DISTINCT user_pseudo_id FROM transactions
    )
  GROUP BY
    user_pseudo_id, session_source_medium, session_id
),

-- Joined table with sessions and transactions
sessions_with_transactions AS (
  SELECT
    st.*, tt.*
  EXCEPT (session_id, user_pseudo_id),
  FROM
    sessions AS st
    LEFT JOIN transactions AS tt
    ON st.user_pseudo_id = tt.user_pseudo_id
    AND st.session_id = tt.session_id
),

-- Final table with additional data and filtering
combined_table AS (
  SELECT
    *,
    COUNT(1) OVER(PARTITION BY user_pseudo_id) AS total_sessions,
    RANK() OVER (
      PARTITION BY user_pseudo_id
      ORDER BY TIMESTAMP_MICROS(session_start_timestamp) ASC
    ) AS session_number
  FROM
    sessions_with_transactions AS swt
  WHERE
    session_start_timestamp <= (
      SELECT MAX(transaction_timestamp)
      FROM sessions_with_transactions
      WHERE swt.user_pseudo_id = user_pseudo_id
    )
    AND TIMESTAMP_MICROS(session_start_timestamp) >= TIMESTAMP_ADD(
      TIMESTAMP_MICROS(
        (
          SELECT MAX(transaction_timestamp)
          FROM sessions_with_transactions
          WHERE swt.user_pseudo_id = user_pseudo_id
        )
      ),
      INTERVAL -attribution_window DAY
    )
  ORDER BY
    user_pseudo_id, session_id
)

-- Result
SELECT
  *
FROM
  combined_table;

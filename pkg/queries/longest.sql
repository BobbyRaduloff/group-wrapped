WITH conv_stats AS (
    SELECT
        conversation_id,
        MIN(msg_timestamp)                           AS conv_start,
        MAX(msg_timestamp)                           AS conv_end,
        MAX(msg_timestamp) - MIN(msg_timestamp)      AS duration,
        array_sort(array_agg(DISTINCT msg_sender))   AS participants
    FROM conversations
    GROUP BY conversation_id
)

SELECT
    conversation_id,
  conv_start,
    duration,
    participants                 -- e.g. ['Alice', 'Bob', 'Charlie']
FROM conv_stats
ORDER BY duration DESC
LIMIT 1;       

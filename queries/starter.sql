
-- Top conversation starter
WITH first_msgs AS (                         -- one row per conversation (its first line)
    SELECT
        conversation_id,
        msg_sender
    FROM (
        SELECT
            conversation_id,
            msg_sender,
            ROW_NUMBER() OVER (
                PARTITION BY conversation_id
                ORDER BY msg_timestamp
            ) AS rn                              -- 1 = first message in that conversation
        FROM conversations
    )
    WHERE rn = 1                                 -- keep only the first message
)

SELECT
    msg_sender         AS conversation_starter,
    COUNT(*)           AS conversations_started
FROM first_msgs
GROUP BY msg_sender
ORDER BY conversations_started DESC
LIMIT 1;                                      

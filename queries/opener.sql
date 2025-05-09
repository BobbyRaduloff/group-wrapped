
/* Who sent the first message of each conversation? */
WITH conv_starters AS (
    SELECT
        conversation_id,
        FIRST_VALUE(msg_sender) OVER (
            PARTITION BY conversation_id
            ORDER BY msg_timestamp
        ) AS starter
    FROM conversations
    /* One row per conversation is enough, so use DISTINCT later */
)

/* Count how many conversations each person started and keep the top one */
SELECT
    starter   AS msg_sender,
    COUNT(*)  AS conversations_started
FROM (
    SELECT DISTINCT conversation_id, starter
    FROM conv_starters
) t
GROUP BY starter
ORDER BY conversations_started DESC
LIMIT 1;          -- remove this LIMIT if you want every sender ranked

-- top‑3 most‑common emoji (single‑line regex)
WITH emoji_tokens AS (
    SELECT
        emoji
    FROM chat
         , LATERAL UNNEST(
               regexp_extract_all(
                   msg_text,
                   '[\x{1F600}-\x{1F64F}\x{1F300}-\x{1F5FF}\x{1F680}-\x{1F6FF}\x{1F1E6}-\x{1F1FF}\x{2600}-\x{26FF}\x{2700}-\x{27BF}\x{1F900}-\x{1F9FF}\x{1FA70}-\x{1FAFF}]'
               )
         ) AS t(emoji)
)
SELECT
    emoji,
    COUNT(*) AS emoji_count
FROM emoji_tokens
GROUP BY emoji
ORDER BY emoji_count DESC
LIMIT 3;



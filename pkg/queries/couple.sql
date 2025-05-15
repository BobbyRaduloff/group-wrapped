-- couple with the most 1‑on‑1 conversations
WITH two_party_conv AS (            -- ① keep only 2‑person conversations
    SELECT
        conversation_id,
        MIN(msg_sender) AS person1, -- canonical order (alphabetical)
        MAX(msg_sender) AS person2
    FROM conversations
    GROUP BY conversation_id
    HAVING COUNT(DISTINCT msg_sender) = 2
)
SELECT
    person1,
    person2,
    COUNT(*) AS one_on_one_conversation_count
FROM two_party_conv
GROUP BY person1, person2
ORDER BY one_on_one_conversation_count DESC
LIMIT 1;                             -- top couple



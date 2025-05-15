/* ❶ Normalise each line: lower-case, change every non-letter to a space   */
WITH cleaned AS (
    SELECT
        msg_sender,
        regexp_replace(lower(msg_text), '[^a-z]', ' ', 'g') AS cleaned_text
    FROM chat
),

/* ❷ Split into words, keep just the hey-style ones                        */
hey_words AS (
    SELECT
        msg_sender,
        word
    FROM cleaned,
    UNNEST(string_split(cleaned_text, ' ')) AS t(word)
    WHERE word ~ '^he+y+$'                     -- he, heyy, heyyyy…
),

/* ❸ Count the y’s in each hey                                             */
y_counts AS (
    SELECT
        msg_sender,
        length(regexp_extract(word, '^he(y+)$', 1)) AS y_cnt
    FROM hey_words
)

/* ❹ Average per user, sort, pick the winner                               */
SELECT
    msg_sender,
    ROUND(AVG(y_cnt), 2) AS avg_y_per_hey
FROM y_counts
GROUP BY msg_sender
ORDER BY avg_y_per_hey DESC
LIMIT 1;

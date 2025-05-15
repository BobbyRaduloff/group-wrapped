CREATE OR REPLACE TABLE raw AS
SELECT trim(regexp_replace(line, '\x{200E}\r\x{00A0}', ' ', 'g')) AS line
FROM read_csv(
    'tests/bg.txt',
    header = False,
    columns = { 'line': 'VARCHAR' },
    ignore_errors = True,
    strict_mode = False
);

ALTER TABLE raw ADD COLUMN is_new BOOL;
UPDATE raw
SET is_new = regexp_matches(
    line,
    '^\[([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}, [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\]'
);


CREATE OR REPLACE TABLE msg_lines AS
SELECT
    *,
    sum(CASE WHEN is_new THEN 1 ELSE 0 END)
        OVER (
            ORDER BY rowid
        ) AS message_id
FROM raw
ORDER BY rowid;

CREATE OR REPLACE TABLE messages AS
SELECT
    message_id,
    string_agg(
        line, '\n'
        ORDER BY rowid
    ) AS full_line
FROM msg_lines
GROUP BY message_id
ORDER BY message_id;

CREATE OR REPLACE TABLE chat AS
SELECT
    strptime(
        regexp_extract(full_line, '^\[([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}, [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\]', 1),
        '%d.%m.%y, %H:%M:%S'
    ) AS msg_timestamp,
    trim(regexp_extract(full_line, '\]\s*(.*?):', 1)) AS msg_sender,
    trim(regexp_extract(full_line, '\]\s*.*?: (.*)$', 1)) AS msg_text
FROM messages;

SELECT * FROM chat LIMIT 10;

-- total messages
SELECT count(*) AS total_messages
FROM chat;

-- messages per person
SELECT
    msg_sender,
    count(*) AS message_count
FROM chat
GROUP BY msg_sender
ORDER BY message_count DESC;

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

-- 3 messages before the “most‑ignored” post + the post itself + the reply
WITH chat_rn AS (                         -- add a running row number
    SELECT *,
           row_number() OVER (ORDER BY msg_timestamp) AS rn
    FROM chat
),

gaps AS (                                 -- compute the gap to the very next post
    SELECT *,
           LEAD(msg_timestamp) OVER (ORDER BY msg_timestamp)                 AS next_ts,
           LEAD(msg_timestamp) OVER (ORDER BY msg_timestamp)
             - msg_timestamp                                                 AS gap
    FROM chat_rn
),

longest AS (                              -- the single biggest gap that has a reply
    SELECT rn
    FROM gaps
    WHERE next_ts IS NOT NULL
    ORDER BY gap DESC
    LIMIT 1
),

context AS (                              -- 3 msgs before, the gap‑maker, and its reply
    SELECT c.*
    FROM chat_rn        AS c
    JOIN longest        AS l
      ON c.rn BETWEEN l.rn - 3   -- three earlier messages (may be fewer at file start)
                  AND l.rn + 1   -- the long‑awaited reply (row right after the gap)
)

SELECT
    msg_timestamp,
    msg_sender,
    msg_text
FROM context
ORDER BY msg_timestamp;                   -- chronological listing


-- images
CREATE OR REPLACE TABLE images AS
SELECT msg_sender
FROM chat
WHERE lower(msg_text) IN ('image omitted', 'images omitted');

SELECT
    msg_sender,
    count(*) AS image_count
FROM images
GROUP BY msg_sender
ORDER BY image_count DESC;

-- videos
CREATE OR REPLACE TABLE videos AS
SELECT msg_sender
FROM chat
WHERE lower(msg_text) IN ('video omitted', 'videos omitted');

SELECT
    msg_sender,
    count(*) AS video_count
FROM videos
GROUP BY msg_sender
ORDER BY video_count DESC;

-- audio
CREATE OR REPLACE TABLE audios AS
SELECT msg_sender
FROM chat
WHERE lower(msg_text) IN ('audio omitted', 'audios omitted');

SELECT
    msg_sender,
    count(*) AS audio_count
FROM audios
GROUP BY msg_sender
ORDER BY audio_count DESC;

-- sticker 
CREATE OR REPLACE TABLE stickers AS
SELECT msg_sender
FROM chat
WHERE lower(msg_text) IN ('sticker omitted', 'stickers omitted');

SELECT
    msg_sender,
    count(*) AS sticker_count
FROM stickers
GROUP BY msg_sender
ORDER BY sticker_count DESC;



-- ❶ messages + conversation_id
CREATE OR REPLACE TABLE conversations AS
WITH ordered AS (
    SELECT
        *,                                              -- keep all columns
        LAG(msg_timestamp) OVER (ORDER BY msg_timestamp) AS prev_ts
    FROM chat
),
flags AS (
    SELECT
        *,
        CASE
            WHEN prev_ts IS NULL
              OR msg_timestamp - prev_ts > INTERVAL '300 seconds'
            THEN 1                                        -- new conversation starts
            ELSE 0
        END AS new_conv
    FROM ordered
)
SELECT
    *,                                                 -- message columns
    SUM(new_conv) OVER (ORDER BY msg_timestamp) AS conversation_id
FROM flags
ORDER BY msg_timestamp;


-- total number of distinct conversations
SELECT COUNT(DISTINCT conversation_id) AS conversation_count
FROM conversations;


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
LIMIT 1;                         -- longest conversation only

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


-- person with the longest consecutive‑message streak (DuckDB‑safe)
WITH ordered AS (          -- global and per‑sender row numbers
    SELECT
        *,
        row_number() OVER (ORDER BY msg_timestamp)                         AS rn,
        row_number() OVER (PARTITION BY msg_sender ORDER BY msg_timestamp) AS rn_by_sender
    FROM chat
),

runs AS (                  -- contiguous same‑sender blocks
    SELECT
        *,
        rn - rn_by_sender AS run_id        -- constant inside each streak
    FROM ordered
),

streaks AS (               -- streak length & boundaries
    SELECT
        msg_sender,
        run_id,
        COUNT(*)                           AS streak_len,
        MIN(msg_timestamp)                 AS streak_start_ts,
        MAX(msg_timestamp)                 AS streak_end_ts
    FROM runs
    GROUP BY msg_sender, run_id
)

SELECT
    msg_sender                          AS streak_sender,
    streak_len,
    CAST(streak_start_ts AS DATE)       AS streak_date,   -- << cast instead of DATE()
    streak_start_ts,
    streak_end_ts
FROM streaks
ORDER BY streak_len DESC, streak_start_ts
LIMIT 1;


-- Person with the most “😭, 😂, or 💀” messages
SELECT
    msg_sender,
    COUNT(*) AS emoji_message_count
FROM chat
WHERE regexp_matches(
          msg_text,
          '[😂😭💀]'            -- 🤣  Unicode literals work fine in DuckDB
      )
GROUP BY msg_sender
ORDER BY emoji_message_count DESC
LIMIT 1;                         -- remove LIMIT to see the full ranking

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
LIMIT 1;                                         -- drop LIMIT to see a full ranking

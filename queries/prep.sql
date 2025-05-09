
--------------------------------------------------------------------
-- 0.  Load the plain-text dump (table `rawest` already exists)
--------------------------------------------------------------------
CREATE OR REPLACE TABLE raw AS
SELECT
    trim(regexp_replace(line, '\x{200E}\x{00A0}\r', ' ', 'g')) AS line
FROM rawest;

--------------------------------------------------------------------
-- 1.  Mark the lines that begin a new message
--------------------------------------------------------------------
ALTER TABLE raw ADD COLUMN is_new BOOL;

UPDATE raw
SET    is_new = regexp_matches(
           line,
           '^\[([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}, [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\]'
       );

--------------------------------------------------------------------
-- 2.  Collapse physical lines into logical message lines
--------------------------------------------------------------------
CREATE OR REPLACE TABLE msg_lines AS
SELECT
    *,
    sum(CASE WHEN is_new THEN 1 ELSE 0 END) OVER (ORDER BY rowid) AS message_id
FROM   raw
ORDER  BY rowid;

--------------------------------------------------------------------
-- 3.  Re-assemble full messages
--------------------------------------------------------------------
CREATE OR REPLACE TABLE messages AS
SELECT
    message_id,
    string_agg(line, '\n' ORDER BY rowid) AS full_line
FROM   msg_lines
GROUP  BY message_id
ORDER  BY message_id;

--------------------------------------------------------------------
-- 4.  Build `chat_raw` (= all messages, still including the system user)
--------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE chat_raw AS
SELECT
    strptime(
        regexp_extract(full_line,
            '^\[([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}, [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\]',
            1),
        '%d.%m.%y, %H:%M:%S'
    )                                              AS msg_timestamp,
    trim(regexp_extract(full_line, '\]\s*(.*?):', 1))    AS msg_sender,
    trim(regexp_extract(full_line, '\]\s*.*?: (.*)$', 1)) AS msg_text
FROM messages;

--------------------------------------------------------------------
-- 5.  Detect the WhatsApp “system” sender(s)
--------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE system_senders AS
WITH patterns(txt) AS (
    VALUES
      ('Messages and calls are end-to-end encrypted%'),
      ('You created group%'),
      ('You changed this group%')
)
SELECT DISTINCT msg_sender
FROM   chat_raw, patterns
WHERE  msg_text ILIKE txt;     -- ILIKE = case-insensitive LIKE

--------------------------------------------------------------------
-- 6.  Final `chat` table  (system sender purged – all downstream SQL is safe)
--------------------------------------------------------------------
CREATE OR REPLACE TABLE chat AS
SELECT *
FROM   chat_raw
WHERE  msg_sender NOT IN (SELECT msg_sender FROM system_senders);

--------------------------------------------------------------------
-- 7.  Media-only helper tables (now fed by the cleaned-up `chat`)
--------------------------------------------------------------------
CREATE OR REPLACE TABLE images AS
SELECT msg_sender
FROM   chat
WHERE  lower(msg_text) IN ('image omitted', 'images omitted');

CREATE OR REPLACE TABLE videos AS
SELECT msg_sender
FROM   chat
WHERE  lower(msg_text) IN ('video omitted', 'videos omitted');

CREATE OR REPLACE TABLE audios AS
SELECT msg_sender
FROM   chat
WHERE  lower(msg_text) IN ('audio omitted', 'audios omitted');

CREATE OR REPLACE TABLE stickers AS
SELECT msg_sender
FROM   chat
WHERE  lower(msg_text) IN ('sticker omitted', 'stickers omitted');

--------------------------------------------------------------------
-- 8.  Conversation segmentation (unchanged logic, but runs on clean `chat`)
--------------------------------------------------------------------
CREATE OR REPLACE TABLE conversations AS
WITH ordered AS (
    SELECT
        *,
        LAG(msg_timestamp) OVER (ORDER BY msg_timestamp) AS prev_ts
    FROM chat
),
flags AS (
    SELECT
        *,
        CASE
            WHEN prev_ts IS NULL
              OR msg_timestamp - prev_ts > INTERVAL '300 seconds'
            THEN 1
            ELSE 0
        END AS new_conv
    FROM ordered
)
SELECT
    *,
    SUM(new_conv) OVER (ORDER BY msg_timestamp) AS conversation_id
FROM flags
ORDER BY msg_timestamp;


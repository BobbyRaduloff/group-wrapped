
-- Average words per message for every user (sender)
SELECT
    msg_sender,
    avg(len(regexp_split_to_array(msg_text, '\s+'))) AS avg_words_per_message
FROM chat
WHERE                 -- skip empty rows & attachment placeholders
      msg_text IS NOT NULL
  AND msg_text <> ''
  AND lower(msg_text) NOT LIKE '% omitted'
GROUP BY msg_sender
ORDER BY avg_words_per_message ASC
LIMIT 1;   -- or ORDER BY msg_sender
                   -- remove LIMIT for a full list

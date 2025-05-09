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
LIMIT 1;        

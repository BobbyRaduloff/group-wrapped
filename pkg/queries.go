package pkg

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "embed"

	duckdb "github.com/marcboeker/go-duckdb"
)

//go:embed queries/prep.sql
var PrepQuery string

//go:embed queries/top3emojis.sql
var Top3EmojisQuery string

//go:embed queries/couple.sql
var CoupleQuery string

//go:embed queries/longest.sql
var LongestConvoQuery string

//go:embed queries/jester.sql
var JesterQuery string

//go:embed queries/starter.sql
var StarterQuery string

//go:embed queries/hey.sql
var HeyQuery string

//go:embed queries/bot.sql
var BotQuery string

//go:embed queries/opener.sql
var OpenerQuery string

// totalMessages returns the total number of messages or an error.
func totalMessages(db *sql.DB) (int, error) {
	var total int
	if err := db.QueryRow("SELECT count(*) AS total_messages FROM chat;").Scan(&total); err != nil {
		return 0, fmt.Errorf("failed to get total messages: %w", err)
	}
	return total, nil
}

// MessagePerPerson describes a sender and their message count.
type MessagePerPerson struct {
	Sender string `json:"sender"`
	Count  int    `json:"count"`
}

// messagesPerPerson returns a slice with message counts per sender or an error.
func messagesPerPerson(db *sql.DB) ([]MessagePerPerson, error) {
	rows, err := db.Query("SELECT msg_sender, count(*) AS message_count FROM chat GROUP BY msg_sender ORDER BY message_count DESC;")
	if err != nil {
		return nil, fmt.Errorf("failed to create messages per person query: %w", err)
	}
	defer rows.Close()

	var ret []MessagePerPerson
	for rows.Next() {
		var m MessagePerPerson
		if err := rows.Scan(&m.Sender, &m.Count); err != nil {
			return nil, fmt.Errorf("failed to scan messages per person: %w", err)
		}
		m.Sender = strings.Replace(m.Sender, "- ", "", 1)
		ret = append(ret, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iteration error for messages per person: %w", err)
	}
	return ret, nil
}

// TopEmoji represents an emoji and its frequency.
type TopEmoji struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
}

// topEmojis queries and returns the top emojis or an error.
func topEmojis(db *sql.DB) ([]TopEmoji, error) {
	rows, err := db.Query(Top3EmojisQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create top 3 emojis query: %w", err)
	}
	defer rows.Close()

	var ret []TopEmoji
	for rows.Next() {
		var t TopEmoji
		if err := rows.Scan(&t.Emoji, &t.Count); err != nil {
			return nil, fmt.Errorf("failed to scan top emoji: %w", err)
		}
		ret = append(ret, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iteration error for top emojis: %w", err)
	}
	return ret, nil
}

// MediaCount holds a sender and their media count.
type MediaCount struct {
	Sender string `json:"sender"`
	Count  int    `json:"count"`
}

// mediaCounter returns counts of a given media type per sender or an error.
func mediaCounter(db *sql.DB, media string) ([]MediaCount, error) {
	rows, err := db.Query("SELECT msg_sender, count(*) AS cnt FROM " + media + " GROUP BY msg_sender ORDER BY cnt DESC;")
	if err != nil {
		return nil, fmt.Errorf("failed to create a media query (%s): %w", media, err)
	}
	defer rows.Close()

	var ret []MediaCount
	for rows.Next() {
		var m MediaCount
		if err := rows.Scan(&m.Sender, &m.Count); err != nil {
			return nil, fmt.Errorf("failed to scan media counter: %w", err)
		}

		m.Sender = strings.Replace(m.Sender, "- ", "", 1)
		ret = append(ret, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iteration error for media counter: %w", err)
	}
	return ret, nil
}

// conversationCount returns the total number of distinct conversations or an error.
func conversationCount(db *sql.DB) (int, error) {
	var cnt int
	if err := db.QueryRow("SELECT count(DISTINCT conversation_id) AS cnt FROM conversations;").Scan(&cnt); err != nil {
		return 0, fmt.Errorf("failed to read total conversations: %w", err)
	}
	return cnt, nil
}

type Couple struct {
	PersonOne string `json:"personOne"`
	PersonTwo string `json:"personTwo"`
	Count     int    `json:"count"`
}

// coupleFinder identifies the top two-person conversation pair.
func coupleFinder(db *sql.DB) (Couple, error) {
	var (
		person1 string
		person2 string
		count   int
	)
	if err := db.QueryRow(CoupleQuery).Scan(&person1, &person2, &count); err != nil {
		return Couple{}, fmt.Errorf("failed to find couple: %w", err)
	}

	person2 = strings.Replace(person2, "- ", "", 1)
	person1 = strings.Replace(person1, "- ", "", 1)
	return Couple{
		person1,
		person2,
		count,
	}, nil
}

type LongestConversation struct {
	Start           time.Time `json:"start"`
	DurationMinutes int       `json:"durationMinutes"`
	Participants    []string  `json:"participants"`
}

// longestConvo finds the longest conversation.
func longestConvo(db *sql.DB) (LongestConversation, error) {
	var (
		id           int
		start        time.Time
		duration     duckdb.Interval
		participants duckdb.Composite[[]string]
	)

	if err := db.QueryRow(LongestConvoQuery).Scan(&id, &start, &duration, &participants); err != nil {
		return LongestConversation{}, fmt.Errorf("failed to get longest convo: %w", err)
	}

	dur, err := time.ParseDuration(fmt.Sprintf("%dms", duration.Micros/1000))
	if err != nil {
		return LongestConversation{}, fmt.Errorf("cannot parse duration: %w", err)
	}

	parts := participants.Get()
	for i, p := range parts {
		parts[i] = strings.Replace(p, "- ", "", 1)
	}

	return LongestConversation{
		start,
		int(dur.Minutes()),
		parts,
	}, nil
}

// jesterFind returns the name of the most humorous participant and their joke count.
func jesterFind(db *sql.DB) (string, int, error) {
	var (
		name  string
		count int
	)
	if err := db.QueryRow(JesterQuery).Scan(&name, &count); err != nil {
		return "", 0, fmt.Errorf("failed to get jester: %w", err)
	}

	name = strings.Replace(name, "- ", "", 1)
	return name, count, nil
}

// convoStarter identifies the most frequent conversation starter.
func convoStarter(db *sql.DB) (string, int, error) {
	var (
		name  string
		count int
	)
	if err := db.QueryRow(StarterQuery).Scan(&name, &count); err != nil {
		return "", 0, fmt.Errorf("failed to get conversation starter: %w", err)
	}

	name = strings.Replace(name, "- ", "", 1)
	return name, count, nil
}

// averageYPerHey calculates the average number of "y"s per "hey" for each sender.
func averageYPerHey(db *sql.DB) (string, float64, error) {
	var (
		name string
		avg  float64
	)
	if err := db.QueryRow(HeyQuery).Scan(&name, &avg); err != nil {
		return "", 0, fmt.Errorf("failed to count heys: %w", err)
	}

	name = strings.Replace(name, "- ", "", 1)
	return name, avg, nil
}

func botFinder(db *sql.DB) (string, float64, error) {
	var (
		name string
		avg  float64
	)
	if err := db.QueryRow(BotQuery).Scan(&name, &avg); err != nil {
		return "", 0, fmt.Errorf("failed to count avg words per mesage: %w", err)
	}

	name = strings.Replace(name, "- ", "", 1)
	return name, avg, nil
}

func openerFinder(db *sql.DB) (string, int, error) {
	var (
		name  string
		count int
	)
	if err := db.QueryRow(OpenerQuery).Scan(&name, &count); err != nil {
		return "", 0, fmt.Errorf("failed to get opener: %w", err)
	}

	name = strings.Replace(name, "- ", "", 1)
	return name, count, nil
}

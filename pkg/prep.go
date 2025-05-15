package pkg

import (
	"database/sql"
	"regexp"
	"strings"
)

// get first line
// clean
// if first line contains [], iphone, else android
// parse iphone as you know
// parse android (?):
//   -- regex for timestamp
//   -- remove system messages

func GetRawLines(content string) []string {
	raw := strings.Split(content, "\n")

	cleaned := []string{}
	for _, row := range raw {
		c := strings.TrimSpace(row)
		c = strings.ReplaceAll(c, "\u200e", "")
		c = strings.ReplaceAll(c, "\u202f", "")

		if len(c) == 0 {
			continue
		}

		cleaned = append(cleaned, c)
	}

	if strings.Contains(cleaned[0], "[") && strings.Contains(cleaned[0], "]") {
		return GetRawLinesIOS(cleaned)
	}

	return GetRawLinesAndroid(cleaned)
}

func GetRawLinesAndroid(raw []string) []string {
	var fileLines []string

	mainLineR := regexp.MustCompile(`^(.{3,100}?) - (.*)`)
	for _, row := range raw {
		matches := mainLineR.FindStringSubmatch(row)

		// skip non-main lines
		if len(matches) <= 1 {
			fileLines = append(fileLines, row)
			continue
		}

		// skip system messages
		if !strings.Contains(matches[2], ":") {
			continue
		}

		ts := strings.ReplaceAll(matches[1], " - ", "")

		parsed, err := ParseFlexible(ts)
		Invariant(err == nil, "cant parse time", err, row)

		out := parsed.Format("02.01.06, 15:04:05")

		newRow := strings.ReplaceAll(row, matches[1], "["+out+"] ")
		fileLines = append(fileLines, newRow)

	}

	return fileLines
}

func GetRawLinesIOS(raw []string) []string {
	var fileLines []string
	r := regexp.MustCompile(`^\[(.+?)\]`)
	for _, row := range raw {
		ts := r.FindString(row)
		tss := strings.Trim(ts, "[]")
		if len(tss) == 0 {
			fileLines = append(fileLines, row)
			continue
		}

		parsed, err := ParseFlexible(tss)
		Invariant(err == nil, "cant parse time", err, row)

		out := parsed.Format("02.01.06, 15:04:05")
		row = strings.ReplaceAll(row, tss, out)
		fileLines = append(fileLines, row)
	}

	return fileLines
}

func PrepDB(db *sql.DB, lines []string) {
	_, err := db.Exec("CREATE OR REPLACE TABLE rawest (line VARCHAR)")
	Invariant(err == nil, "failed to create rawest table", err)

	stmt, err := db.Prepare("INSERT INTO rawest VALUES (?)")
	Invariant(err == nil, "failed to set up rawest insert statement", err)
	for _, line := range lines {
		trimmed := strings.Trim(line, "\r")
		_, err := stmt.Exec(trimmed)
		Invariant(err == nil, "failed to insert line into rawest", trimmed, err)
	}
	err = stmt.Close()
	Invariant(err == nil, "failed to close insert rawest statement", err)

	_, err = db.Exec(PrepQuery)
	Invariant(err == nil, "failed to create raw table", err)
}

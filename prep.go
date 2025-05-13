package main

import (
	"database/sql"
	"regexp"
	"strings"
)

func getRawLines(content string) []string {
	raw := strings.Split(content, "\n")

	var fileLines []string
	r := regexp.MustCompile(`^\[(.+?)\]`)
	for _, row := range raw {
		l := strings.TrimSpace(row)
		l = strings.ReplaceAll(l, "\u200e", "")
		l = strings.ReplaceAll(l, "\u202f", "")

		if len(l) == 0 {
			continue
		}

		ts := r.FindString(l)
		tss := strings.Trim(ts, "[]")
		if len(tss) == 0 {
			fileLines = append(fileLines, l)
			continue
		}

		parsed, err := ParseFlexible(tss)
		invariant(err == nil, "cant parse time", err, l)

		out := parsed.Format("02.01.06, 15:04:05")
		l = strings.ReplaceAll(l, tss, out)
		fileLines = append(fileLines, l)
	}


	return fileLines
}

func prepDB(db *sql.DB, lines []string) {
	_, err := db.Exec("CREATE OR REPLACE TABLE rawest (line VARCHAR)")
	invariant(err == nil, "failed to create rawest table", err)

	stmt, err := db.Prepare("INSERT INTO rawest VALUES (?)")
	invariant(err == nil, "failed to set up rawest insert statement", err)
	for _, line := range lines {
		trimmed := strings.Trim(line, "\r")
		_, err := stmt.Exec(trimmed)
		invariant(err == nil, "failed to insert line into rawest", trimmed, err)
	}
	err = stmt.Close()
	invariant(err == nil, "failed to close insert rawest statement", err)

	_, err = db.Exec(PrepQuery)
	invariant(err == nil, "failed to create raw table", err)
}

package main

import (
	"bufio"
	"database/sql"
	"os"
	"regexp"
	"strings"
)

func getRawLines() []string {
	path := "tests/bg.txt"
	readFile, err := os.Open(path)
	invariant(err == nil, "failed to read file", err)

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	var fileLines []string

	r := regexp.MustCompile(`^\[(.+?)\]`)
	for fileScanner.Scan() {
		l := strings.ReplaceAll(fileScanner.Text(), "\u200e", "")
		l = strings.ReplaceAll(l, "\u202f", "")

		ts := r.FindString(l)
		tss := strings.Trim(ts, "[]")
		if len(tss) == 0 {
			fileLines = append(fileLines, l)
			continue
		}

		parsed, err := ParseFlexible(tss)
		invariant(err == nil, "cant parse time", err, l)

		out := parsed.Format("06.01.02, 15:04:05")
		l = strings.ReplaceAll(l, tss, out)
		fileLines = append(fileLines, l)
	}

	readFile.Close()

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

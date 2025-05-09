package main

import (
	"database/sql"
	_ "embed"
	"log"

	"github.com/kr/pretty"
)

func main() {
	db, err := sql.Open("duckdb", "")
	invariant(err == nil, "failed to connect to duckdb", err)
	defer db.Close()

	rawLines := getRawLines()
	prepDB(db, rawLines)

	stats := GetStats(db)
	log.Printf("%# v\n", pretty.Formatter(stats))

	cards := AssignCards(db, stats)
	log.Printf("%# v\n", pretty.Formatter(cards))
}

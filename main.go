package main

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
)

type Output struct {
	Statistics Stats  `json:"statistics"`
	Cards      []Card `json:"cards"`
}

func main() {
	db, err := sql.Open("duckdb", "")
	invariant(err == nil, "failed to connect to duckdb", err)
	defer db.Close()

	rawLines := getRawLines()
	prepDB(db, rawLines)

	stats := GetStats(db)
	cards := AssignCards(db, stats)

	out := Output{
		stats, cards,
	}

	outjson, _ := json.Marshal(out)
	fmt.Println(string(outjson))
}

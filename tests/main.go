package main

import (
	"database/sql"
	_ "embed"
	"group-wrapped/pkg"
	"log"
)

//go:embed albert.txt
var TAlbert string

//go:embed android.txt
var TAndroid string

//go:embed bg.txt
var TBG string

//go:embed uk.txt
var TUK string

//go:embed us.txt
var TUS string

func main() {
	tests := []string{
		TAlbert, TAndroid, TBG, TUK, TUS,
	}

	for i, s := range tests {
		log.Println(i)
		rawLines := pkg.GetRawLines(s)
		db, err := sql.Open("duckdb", "")
		pkg.Invariant(err == nil, "failed to connect to duckdb", err)
		defer db.Close()
		pkg.PrepDB(db, rawLines)

		stats := pkg.GetStats(db)
		cards := pkg.AssignCards(db, stats)

		log.Println(stats, cards)
	}
}

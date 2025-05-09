package main

import (
	"database/sql"
	"math"
	"math/rand"
	"slices"
)

// grandma: most stickers
// opener: started most convos
// the bot: least average words per message
// jester: most laughing emojis
// lurker: least messages sent
// spammer: most images and videos and audio sent
// core: most messages sent
// tim cheese: random drop
// basic bitch: most ending y's on hey on average

var CardTypes = []string{
	"GRANDMA", "OPENER", "BOT",
	"JESTER", "LURKER", "SPAMMER",
	"CORE", "BASICBITCH",
}

type Card struct {
	Person string `json:"person"`
	Type   string `json:"type"`
	Value  int    `json:"value"`
}

func AssignCards(db *sql.DB, stats Stats) []Card {
	cards := make(map[string]*Card)

	largestStickerSender := ""
	largestStickerCount := 0
	for _, x := range stats.StickersPerPerson {
		if x.Count > largestStickerCount {
			largestStickerCount = x.Count
			largestStickerSender = x.Sender
		}
	}
	if largestStickerCount > 0 {
		cards["GRANDMA"] = &Card{
			largestStickerSender,
			"GRANDMA",
			largestStickerCount,
		}
	}

	opener, count, err := openerFinder(db)
	if err == nil {
		cards["OPENER"] = &Card{
			opener,
			"OPENER",
			count,
		}
	}

	bot, avg, err := botFinder(db)
	if err == nil {
		cards["BOT"] = &Card{
			bot,
			"BOT",
			int(math.Round(avg)),
		}
	}

	jester, count, err := jesterFind(db)
	if err == nil {
		cards["JESTER"] = &Card{
			jester,
			"JESTER",
			count,
		}
	}

	cards["LURKER"] = &Card{
		stats.MessagesPerPerson[len(stats.MessagesPerPerson)-1].Sender,
		"LURKER",
		stats.MessagesPerPerson[len(stats.MessagesPerPerson)-1].Count,
	}

	mediaCounts := make(map[string]int)
	for i := range len(stats.MessagesPerPerson) {
		mediaCounts[stats.MessagesPerPerson[i].Sender] = 0
	}

	for _, v := range stats.AudioPerPerson {
		mediaCounts[v.Sender] += v.Count
	}
	for _, v := range stats.ImagesPerPerson {
		mediaCounts[v.Sender] += v.Count
	}
	for _, v := range stats.VideosPerPerson {
		mediaCounts[v.Sender] += v.Count
	}

	spammer := ""
	spammerCount := 0
	for k, v := range mediaCounts {
		if v > spammerCount {
			spammer = k
			spammerCount = v
		}
	}

	if spammerCount > 0 {
		cards["SPAMMER"] = &Card{
			spammer,
			"SPAMMER",
			spammerCount,
		}
	}

	cards["CORE"] = &Card{
		stats.MessagesPerPerson[0].Sender,
		"CORE",
		stats.MessagesPerPerson[0].Count,
	}

	basic, avg, err := averageYPerHey(db)
	if err == nil && avg > 2 {
		cards["BASICBITCH"] = &Card{
			basic,
			"BASICBITCH",
			int(math.Round(avg)),
		}
	}

	calculatedCards := []Card{}
	for _, v := range cards {
		if v != nil {
			calculatedCards = append(calculatedCards, *v)
		}
	}

	rand.Shuffle(len(calculatedCards), func(i, j int) { calculatedCards[i], calculatedCards[j] = calculatedCards[j], calculatedCards[i] })
	usedCards := []string{}
	cardCount := min(len(calculatedCards), min(len(stats.MessagesPerPerson), 5))
	ret := []Card{}

	for _, p := range stats.MessagesPerPerson {
		if len(ret) >= cardCount {
			break
		}

		if !slices.Contains(usedCards, "TIMECHEESE") {
			chance := rand.Int63n(10000)
			if chance == 1337 {
				usedCards = append(usedCards, "TIMECHEESE")
				ret = append(ret, Card{
					p.Sender,
					"TIMECHEESE",
					0,
				})
				continue
			}
		}

		var firstMatch *Card
		for _, v := range calculatedCards {
			if v.Person == p.Sender && !slices.Contains(usedCards, v.Type) {
				firstMatch = &v
				usedCards = append(usedCards, v.Type)
				break
			}
		}
		if firstMatch == nil {
			continue
		}

		ret = append(ret, *firstMatch)
	}

	return ret
}

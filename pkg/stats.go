package pkg

import "database/sql"

type Stats struct {
	TotalMessages      int                `json:"totalMessages"`
	MessagesPerPerson  []MessagePerPerson `json:"messagesPerPerson"`
	Top3Emojis         []TopEmoji         `json:"top3emojis"`
	ImagesPerPerson    []MediaCount       `json:"imagesPerPerson"`
	VideosPerPerson    []MediaCount       `json:"videosPerPerson"`
	AudioPerPerson     []MediaCount       `json:"AudioPerPerson"`
	StickersPerPerson  []MediaCount       `json:"stickersPerPerson"`
	TotalConversations int                `json:"totalConversations"`
	Duo                Couple             `json:"couple"`
}

func GetStats(db *sql.DB) Stats {
	ret := Stats{}

	total, err := totalMessages(db)
	if err == nil {
		ret.TotalMessages = total
	}

	perPerson, err := messagesPerPerson(db)
	if err == nil {
		ret.MessagesPerPerson = perPerson
	}

	top, err := topEmojis(db)
	if err == nil {
		ret.Top3Emojis = top
	}

	images, err := mediaCounter(db, "images")
	if err == nil {
		ret.ImagesPerPerson = images
	}

	videos, err := mediaCounter(db, "videos")
	if err == nil {
		ret.VideosPerPerson = videos
	}

	audios, err := mediaCounter(db, "audios")
	if err == nil {
		ret.AudioPerPerson = audios
	}

	stickers, err := mediaCounter(db, "stickers")
	if err == nil {
		ret.StickersPerPerson = stickers
	}

	total, err = conversationCount(db)
	if err == nil {
		ret.TotalConversations = total
	}

	duo, err := coupleFinder(db)
	if err == nil {
		ret.Duo = duo
	}

	return ret
}

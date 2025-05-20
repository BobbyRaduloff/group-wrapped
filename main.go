package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	_ "embed"
	"fmt"
	"group-wrapped/pkg"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
)

type Output struct {
	Statistics pkg.Stats  `json:"statistics"`
	Cards      []pkg.Card `json:"cards"`
}

const (
	maxUpload       = 1 << 20 // 5 MB  compressed
	maxUncompressed = 3 << 20 // 10 MB uncompressed
)

var dataMu sync.Mutex

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	r := gin.Default()
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://192.168.1.41:5173", "https://whatswrapped.me", "http://localhost:5173"},
		AllowMethods:     []string{"GET", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"},
		AllowHeaders:     []string{"Origin"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	r.GET("/chats", func(c *gin.Context) {
		dataMu.Lock()
		defer dataMu.Unlock()

		data, err := os.ReadFile("chats.txt")
		if err != nil {
			c.Data(200, "text/plain", []byte(data))
			return
		}

		c.Data(200, "text/plain", []byte(data))
	})

	r.POST("/", func(c *gin.Context) {
		upFile, hdr, err := c.Request.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, "Please upload a file.")
			return
		}
		defer upFile.Close()

		limited := http.MaxBytesReader(c.Writer, upFile, maxUpload)

		ext := strings.ToLower(filepath.Ext(hdr.Filename))
		var txt string

		switch ext {

		case ".txt":
			body, err := io.ReadAll(&io.LimitedReader{R: limited, N: maxUncompressed})
			if err != nil {
				c.JSON(http.StatusBadRequest, "Unable to read the file.")
				return
			}
			txt = string(body)

		case ".zip":
			zipBytes, err := io.ReadAll(limited)
			if err != nil {
				c.JSON(http.StatusBadRequest,
					"Please upload a WhatsApp chat export: .zip or .txt (wihtout media). (1)")
				return
			}

			zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
			if err != nil {
				c.JSON(http.StatusBadRequest,
					"Please upload a WhatsApp chat export: .zip or .txt (wihtout media). (2)")
				return
			}

			if len(zr.File) < 1 {
				c.JSON(http.StatusBadRequest,
					"Please upload a WhatsApp chat export: .zip or .txt (wihtout media). (3)")
				return
			}

			zf := zr.File[0]
			if !strings.HasSuffix(strings.ToLower(zf.Name), ".txt") {
				c.JSON(http.StatusBadRequest,
					"Please upload a WhatsApp chat export: .zip or .txt (wihtout media). (4)")
				return
			}
			if zf.UncompressedSize64 > maxUncompressed {
				c.JSON(http.StatusBadRequest,
					"Please upload a WhatsApp chat export: .zip or .txt (wihtout media). (5)")
				return
			}

			rc, err := zf.Open()
			if err != nil {
				c.JSON(http.StatusBadRequest,
					"Please upload a WhatsApp chat export: .zip or .txt (wihtout media). (6)")
				return
			}
			defer rc.Close()

			body, err := io.ReadAll(&io.LimitedReader{R: rc, N: maxUncompressed})
			if err != nil {
				c.JSON(http.StatusBadRequest,
					"Please upload a WhatsApp chat export: .zip or .txt (wihtout media). (7)")
				return
			}
			txt = string(body)

		default:
			c.JSON(http.StatusBadRequest,
				"Please upload a WhatsApp chat export: .zip or .txt (wihtout media). (8)")
			return
		}

		id := uuid.New().String()
		fn := id + ".txt"
		pkg.DumpToR2(fn, []byte(txt))

		rawLines := pkg.GetRawLines(txt)
		db, err := sql.Open("duckdb", "")
		pkg.Invariant(err == nil, "failed to connect to duckdb", err)
		defer db.Close()
		pkg.PrepDB(db, rawLines)

		stats := pkg.GetStats(db)
		cards := pkg.AssignCards(db, stats)

		out := Output{
			stats, cards,
		}

		c.JSON(http.StatusOK, out)
	})

	c := cron.New()

	c.AddFunc("*/5 * * * *", func() {
		dataMu.Lock()
		defer dataMu.Unlock()

		read, err := os.ReadFile("chats.txt")
		if err != nil {
			return
		}

		readParsed, err := strconv.Atoi(string(read))
		if err != nil {
			return
		}

		readParsed += rand.Intn(200)

		f, err := os.OpenFile("chats.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return
		}

		f.Truncate(0)
		f.Seek(0, 0)
		fmt.Fprintf(f, "%d", readParsed)

		defer f.Close()
	})
	c.Start()

	r.Run()
}

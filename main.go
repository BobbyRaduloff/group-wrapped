package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	_ "embed"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type Output struct {
	Statistics Stats  `json:"statistics"`
	Cards      []Card `json:"cards"`
}

const (
	maxUpload       = 1 << 20 // 5 MB  compressed
	maxUncompressed = 3 << 20 // 10 MB uncompressed
)

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
		DumpToR2(fn, []byte(txt))

		rawLines := getRawLines(txt)
		db, err := sql.Open("duckdb", "")
		invariant(err == nil, "failed to connect to duckdb", err)
		defer db.Close()
		prepDB(db, rawLines)

		stats := GetStats(db)
		cards := AssignCards(db, stats)

		out := Output{
			stats, cards,
		}

		c.JSON(http.StatusOK, out)
	})

	r.Run()
}

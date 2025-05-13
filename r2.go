package main

import (
	"bytes"
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func DumpToR2(filename string, data []byte) {
	access_key := os.Getenv("R2_ACCESS_KEY")
	secret_key := os.Getenv("R2_SECRET_KEY")
	endpoint := os.Getenv("R2_ENDPOINT")
	bucket := os.Getenv("R2_BUCKET")

	// Configure R2 client
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			access_key,
			secret_key,
			"",
		)),
	)
	if err != nil {
		log.Println(err)
	}

	// Create S3 client with R2-specific endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	ct := "text/plain"
	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &filename,
		Body:   bytes.NewReader(data),
		ContentType: &ct,
	})
	if err != nil {
		log.Println(err)
	}
}

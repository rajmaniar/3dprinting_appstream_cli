package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/appstream"
)

func main() {
	stackName := flag.String("stack", "", "The name of the AppStream stack")
	userName := flag.String("user", "", "The user name")
	fleetName := flag.String("fleet", "", "Fleet name")
	flag.Parse()

	if *stackName == "" || *userName == "" || *fleetName == "" {
		log.Fatal("All flags are required")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	client := appstream.NewFromConfig(cfg)

	url, err := createStreamingURL(client, *stackName, *userName, *fleetName)
	if err != nil {
		log.Fatalf("Failed to create AppStream streaming URL: %v", err)
	}

	fmt.Printf("Streaming URL: %s\n", url)
}

func createStreamingURL(client *appstream.Client, stackName, userName, fleetName string) (string, error) {
	input := &appstream.CreateStreamingURLInput{
		StackName: &stackName,
		UserId:    &userName,
		FleetName: &fleetName,
	}

	result, err := client.CreateStreamingURL(context.TODO(), input)
	if err != nil {
		return "", err
	}

	return *result.StreamingURL, nil
}

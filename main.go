package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"log"
)

func main() {
	stackName := flag.String("stack", "3dPrinting", "The name of the AppStream stack")
	userName := flag.String("user", "3dp", "user name prefix")
	fleetName := flag.String("fleet", "3d_Printing", "Fleet name")
	count := flag.Int64("count", 1, "Number of urls")
	profileName := flag.String("profile", "personal", "AWS profile name")
	flag.Parse()

	if *stackName == "" || *userName == "" || *profileName == "" || *fleetName == "" {
		log.Fatal("All flags are required")
	}

	ctx := context.Background()

	profile := config.WithSharedConfigProfile(*profileName)
	region := config.WithRegion("us-west-2")
	cfg, err := config.LoadDefaultConfig(ctx, profile, region)
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	client := appstream.NewFromConfig(cfg)

	if err := startFleet(ctx, client, *fleetName); err != nil {
		log.Fatalf("startfleet failed with %v", err)
	}

	for i := 0; i < int(*count); i++ {
		url, err := createStreamingURL(ctx, client, *stackName, *userName, *fleetName)
		if err != nil {
			log.Fatalf("Failed to create AppStream streaming URL: %v", err)
		}

		fmt.Printf("Streaming URL: %s\n", url)
	}
}

func startFleet(ctx context.Context, client *appstream.Client, fleetName string) error {
	fleet := &appstream.DescribeFleetsInput{Names: []string{fleetName}}
	fleets, err := client.DescribeFleets(ctx, fleet)
	if err != nil {
		return fmt.Errorf("DescribeFleets failed with err=%w", err)
	}
	for _, f := range fleets.Fleets {
		if *f.Name == fleetName {
			switch f.State {
			case types.FleetStateRunning:
				return nil
			case types.FleetStateStopped:
				fmt.Printf("Starting Fleet %v\n", *f.Name)
				_, err := client.StartFleet(ctx, &appstream.StartFleetInput{Name: &fleetName})
				if err != nil {
					return fmt.Errorf("StartFleet failed with err=%w", err)
				}
				fmt.Printf("Started fleet")
				return nil
			default:
				log.Fatalf("The Fleet state is %v, let that finish and try again...", f.State)
			}
		}
	}
	return nil
}

func createStreamingURL(ctx context.Context, client *appstream.Client, stackName, userName, fleetName string) (string, error) {
	input := &appstream.CreateStreamingURLInput{
		StackName: &stackName,
		UserId:    &userName,
		FleetName: &fleetName,
	}

	result, err := client.CreateStreamingURL(ctx, input)
	if err != nil {
		return "", err
	}

	return *result.StreamingURL, nil
}

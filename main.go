package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"log"
	"os"
	"strings"
)

func main() {
	stackName := flag.String("stack", "3dPrinting_linux", "The name of the AppStream stack")
	userName := flag.String("user", "3dp", "user name")
	fleetName := flag.String("fleet", "3d_Printing_linux", "Fleet name")
	profileName := flag.String("profile", "personal", "AWS profile name")
	studentFileName := flag.String("csv", "", "csv file of students names, creates one stream url per student")
	stop := flag.Bool("stop", false, "Stops the fleet")
	prewarmCapacity := flag.Int("prewarm", 0, "Pre-warm the fleet with this many instances")
	flag.Parse()

	if *stackName == "" || *userName == "" || *profileName == "" || *fleetName == "" {
		log.Fatal("All flags are required")
	}

	streamURLCount := 1

	var studentNames []string
	if *studentFileName != "" {
		names, err := loadStudentsFromFile(studentFileName)
		if err != nil {
			log.Fatalf("failed to load student file name, %v", err)
		}
		studentNames = names
		streamURLCount = len(studentNames)
	}

	ctx := context.Background()

	profile := config.WithSharedConfigProfile(*profileName)
	region := config.WithRegion("us-west-2")
	cfg, err := config.LoadDefaultConfig(ctx, profile, region)
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	client := appstream.NewFromConfig(cfg)

	if *stop {
		if err := stopFleet(ctx, client, *fleetName); err != nil {
			log.Fatalf("stopFleet failed with %v", err)
		}
		log.Printf("Fleet stopping...")
		return
	}
	if *prewarmCapacity > 0 {
		if err := setCapacity(ctx, client, *fleetName, int32(*prewarmCapacity)); err != nil {
			log.Fatalf("setCapacity failed with %v", err)
		}
		log.Printf("Fleet Capacity Set to %v", *prewarmCapacity)
		return
	}
	if err := startFleet(ctx, client, *fleetName); err != nil {
		log.Fatalf("startfleet failed with %v", err)
	}

	for i := 0; i < streamURLCount; i++ {
		uN := fmt.Sprintf("%s-%d", *userName, i)
		if studentNames != nil {
			uN = fmt.Sprintf("%s-%s", *userName, studentNames[i])
		}

		url, err := createStreamingURL(ctx, client, *stackName, uN, *fleetName)
		if err != nil {
			log.Fatalf("Failed to create AppStream streaming URL: %v", err)
		}
		if studentNames != nil {
			fmt.Printf("Slicer for %s: %s\n", studentNames[i], url)
		} else {
			fmt.Printf("Slicer #%d: %s\n", i, url)
		}

	}
}

func loadStudentsFromFile(fileName *string) ([]string, error) {
	file, err := os.Open(*fileName)
	if err != nil {
		return nil, fmt.Errorf("os.Open failed %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	lines, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reader.ReadAll failed %w", err)
	}
	var names []string
	for _, line := range lines {
		names = append(names, strings.TrimSpace(strings.Replace(line[0], " ", "-", -1)))
	}
	return names, nil
}

func setCapacity(ctx context.Context, client *appstream.Client, fleetName string, capacity int32) error {
	updateFleetInput := &appstream.UpdateFleetInput{
		Name: &fleetName,
		ComputeCapacity: &types.ComputeCapacity{
			DesiredInstances: &capacity,
		},
	}
	if output, err := client.UpdateFleet(ctx, updateFleetInput); err != nil {
		return fmt.Errorf("client.UpdateFleet failed with %w", err)
	} else {

		fmt.Printf("Desired Capacity of %s updated -- running %d of %d desired", fleetName, *output.Fleet.ComputeCapacityStatus.Running, *output.Fleet.ComputeCapacityStatus.Desired)
	}
	return nil
}

func stopFleet(ctx context.Context, client *appstream.Client, fleetName string) error {
	fleetD := &appstream.DescribeFleetsInput{Names: []string{fleetName}}
	fleets, err := client.DescribeFleets(ctx, fleetD)
	if err != nil {
		return fmt.Errorf("DescribeFleets failed with err=%w", err)
	}
	for _, f := range fleets.Fleets {
		if *f.Name == fleetName {
			fmt.Printf("Fleet Status is %v with %d running\n", f.State, *f.ComputeCapacityStatus.Running)
		}
	}
	fleet := &appstream.StopFleetInput{Name: &fleetName}
	if _, err := client.StopFleet(ctx, fleet); err != nil {
		return fmt.Errorf("StopFleet failed with err=%w", err)
	}
	return nil
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
				fmt.Printf("Fleet is running already with %d instances of %d desired\n", *f.ComputeCapacityStatus.Running, *f.ComputeCapacityStatus.Desired)
				return nil
			case types.FleetStateStopped:
				fmt.Printf("Starting Fleet %v\n", *f.Name)
				_, err := client.StartFleet(ctx, &appstream.StartFleetInput{Name: &fleetName})
				if err != nil {
					return fmt.Errorf("StartFleet failed with err=%w", err)
				}
				fmt.Printf("Started fleet. This will take some time so try again about 10mins\n\n")
				os.Exit(0)
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
		Validity:  aws.Int64(4 * 60 * 60), //links are valid for 4hrs
	}

	result, err := client.CreateStreamingURL(ctx, input)
	if err != nil {
		return "", err
	}

	return *result.StreamingURL, nil
}

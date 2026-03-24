package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	stackName := flag.String("stack", "3dPrinting_linux", "The name of the AppStream stack")
	userName := flag.String("user", "3dp", "user name")
	fleetName := flag.String("fleet", "3d_Printing_linux", "Fleet name")
	profileName := flag.String("profile", "personal", "AWS profile name")
	studentFileName := flag.String("csv", "", "csv file of students names, creates one stream url per student")
	stop := flag.Bool("stop", false, "Stops the fleet")
	prewarmCapacity := flag.Int("prewarm", 0, "Pre-warm the fleet with this many instances")
	getS3paths := flag.Bool("get-s3paths", false, "Get the s3 paths of the fleet")
	listGcode := flag.Bool("list-gcode", false, "List all .gcode files in user home folders")
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
	if *getS3paths || *listGcode {
		s3Client := s3.NewFromConfig(cfg)
		if *listGcode {
			if err := listGcodeFiles(ctx, client, s3Client, *stackName); err != nil {
				log.Fatalf("listGcodeFiles failed with %v", err)
			}
		} else {
			if err := getS3Buckets(ctx, client, s3Client, *stackName, *userName, studentNames); err != nil {
				log.Fatalf("getS3Buckets failed with %v", err)
			}
		}
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

func listGcodeFiles(ctx context.Context, client *appstream.Client, s3Client *s3.Client, stackName string) error {
	describeInput := &appstream.DescribeStacksInput{
		Names: []string{stackName},
	}

	describeOutput, err := client.DescribeStacks(ctx, describeInput)
	if err != nil {
		return fmt.Errorf("failed to describe stack: %w", err)
	}

	if len(describeOutput.Stacks) == 0 {
		return fmt.Errorf("no stacks found with name: %s", stackName)
	}

	stack := describeOutput.Stacks[0]

	// Find the HOMEFOLDER storage connector
	var s3Bucket string
	for _, connector := range stack.StorageConnectors {
		if connector.ConnectorType == types.StorageConnectorTypeHomefolders && connector.ResourceIdentifier != nil {
			s3Bucket = *connector.ResourceIdentifier
			break
		}
	}

	if s3Bucket == "" {
		return fmt.Errorf("no HOMEFOLDER storage connector found for stack: %s", stackName)
	}

	fmt.Printf("Searching for .gcode files in S3 bucket: %s\n\n", s3Bucket)

	// List all objects with .gcode extension
	listInput := &s3.ListObjectsV2Input{
		Bucket: &s3Bucket,
		Prefix: aws.String("user/custom/"),
	}

	type GcodeFile struct {
		Key          string
		Size         int64
		LastModified *time.Time
		UserHash     string
	}

	var gcodeFiles []GcodeFile

	// Paginate through all objects
	paginator := s3.NewListObjectsV2Paginator(s3Client, listInput)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, ".gcode") {
				// Extract user hash from path: user/custom/<hash>/...
				parts := strings.Split(*obj.Key, "/")
				userHash := ""
				if len(parts) >= 3 {
					userHash = parts[2]
				}

				size := int64(0)
				if obj.Size != nil {
					size = *obj.Size
				}
				gcodeFiles = append(gcodeFiles, GcodeFile{
					Key:          *obj.Key,
					Size:         size,
					LastModified: obj.LastModified,
					UserHash:     userHash,
				})
			}
		}
	}

	if len(gcodeFiles) == 0 {
		fmt.Printf("No .gcode files found.\n")
		return nil
	}

	// Sort by last modified time (oldest first, newest last)
	sort.Slice(gcodeFiles, func(i, j int) bool {
		if gcodeFiles[i].LastModified == nil {
			return true
		}
		if gcodeFiles[j].LastModified == nil {
			return false
		}
		return gcodeFiles[i].LastModified.Before(*gcodeFiles[j].LastModified)
	})

	fmt.Printf("Found %d .gcode file(s):\n\n", len(gcodeFiles))

	// Group by user hash
	userFiles := make(map[string][]GcodeFile)
	for _, file := range gcodeFiles {
		userFiles[file.UserHash] = append(userFiles[file.UserHash], file)
	}

	// Sort user hashes by most recent file (oldest first, newest last)
	var sortedHashes []string
	for hash := range userFiles {
		sortedHashes = append(sortedHashes, hash)
	}
	sort.Slice(sortedHashes, func(i, j int) bool {
		filesI := userFiles[sortedHashes[i]]
		filesJ := userFiles[sortedHashes[j]]
		if len(filesI) == 0 || filesI[0].LastModified == nil {
			return true
		}
		if len(filesJ) == 0 || filesJ[0].LastModified == nil {
			return false
		}
		return filesI[0].LastModified.Before(*filesJ[0].LastModified)
	})

	for _, userHash := range sortedHashes {
		files := userFiles[userHash]
		fmt.Printf("User Hash: %s (%d files)\n", userHash, len(files))
		fmt.Printf("  S3 URL: https://us-west-2.console.aws.amazon.com/s3/buckets/%s?region=us-west-2&prefix=user/custom/%s/&showversions=false\n\n", s3Bucket, userHash)

		for _, file := range files {
			fileName := strings.TrimPrefix(file.Key, fmt.Sprintf("user/custom/%s/", userHash))
			sizeKB := float64(file.Size) / 1024
			timeStr := "unknown"
			if file.LastModified != nil {
				timeStr = file.LastModified.Format("2006-01-02 15:04:05")
			}
			fmt.Printf("    %-50s %8.1f KB  %s\n", fileName, sizeKB, timeStr)
		}
		fmt.Printf("\n")
	}

	return nil
}

func getS3Buckets(ctx context.Context, client *appstream.Client, s3Client *s3.Client, stackName string, userName string, studentNames []string) error {
	describeInput := &appstream.DescribeStacksInput{
		Names: []string{stackName},
	}

	describeOutput, err := client.DescribeStacks(ctx, describeInput)
	if err != nil {
		return fmt.Errorf("failed to describe stack: %w", err)
	}

	if len(describeOutput.Stacks) == 0 {
		return fmt.Errorf("no stacks found with name: %s", stackName)
	}

	stack := describeOutput.Stacks[0]

	// Find the HOMEFOLDER storage connector
	var s3Bucket string
	for _, connector := range stack.StorageConnectors {
		if connector.ConnectorType == types.StorageConnectorTypeHomefolders && connector.ResourceIdentifier != nil {
			s3Bucket = *connector.ResourceIdentifier
			break
		}
	}

	if s3Bucket == "" {
		return fmt.Errorf("no HOMEFOLDER storage connector found for stack: %s", stackName)
	}

	// Get active sessions to try to map usernames to S3 paths
	sessionsInput := &appstream.DescribeSessionsInput{
		StackName: &stackName,
	}

	sessionsOutput, err := client.DescribeSessions(ctx, sessionsInput)
	if err != nil {
		return fmt.Errorf("failed to describe sessions: %w", err)
	}

	fmt.Printf("Active Sessions:\n")
	if len(sessionsOutput.Sessions) == 0 {
		fmt.Printf("  No active sessions found.\n\n")
	} else {
		for _, session := range sessionsOutput.Sessions {
			if session.UserId != nil {
				fmt.Printf("  User: %s (State: %s, Connected: %s)\n",
					*session.UserId,
					session.State,
					session.ConnectionState)
			}
		}
		fmt.Printf("\n")
	}

	// List the actual directories in S3 to get the hash mappings
	listInput := &s3.ListObjectsV2Input{
		Bucket:    &s3Bucket,
		Prefix:    aws.String("user/custom/"),
		Delimiter: aws.String("/"),
	}

	listOutput, err := s3Client.ListObjectsV2(ctx, listInput)
	if err != nil {
		return fmt.Errorf("failed to list S3 objects: %w", err)
	}

	// Extract the hashes from the common prefixes
	var hashDirs []string
	for _, prefix := range listOutput.CommonPrefixes {
		if prefix.Prefix != nil {
			// Extract hash from "user/custom/<hash>/"
			parts := strings.Split(strings.TrimSuffix(*prefix.Prefix, "/"), "/")
			if len(parts) == 3 {
				hashDirs = append(hashDirs, parts[2])
			}
		}
	}

	if len(hashDirs) == 0 {
		fmt.Printf("No user home folders found in S3 bucket yet.\n")
		fmt.Printf("\nNote: S3 home folders are created when users first log in to AppStream.\n")
		return nil
	}

	fmt.Printf("S3 Home Folders found (%d):\n\n", len(hashDirs))

	// Try to identify users by checking for identifying files in their directories
	for _, hash := range hashDirs {
		s3Path := fmt.Sprintf("s3://%s/user/custom/%s", s3Bucket, hash)

		// List a few files in this directory to help identify the user
		filesInput := &s3.ListObjectsV2Input{
			Bucket:  &s3Bucket,
			Prefix:  aws.String(fmt.Sprintf("user/custom/%s/", hash)),
			MaxKeys: aws.Int32(5),
		}

		filesOutput, err := s3Client.ListObjectsV2(ctx, filesInput)
		if err == nil && len(filesOutput.Contents) > 0 {
			fmt.Printf("Hash: %s\n", hash)
			fmt.Printf("  Path: %s\n", s3Path)
			fmt.Printf("  Sample files:\n")
			for _, obj := range filesOutput.Contents {
				if obj.Key != nil {
					// Show just the filename, not the full path
					fileName := strings.TrimPrefix(*obj.Key, fmt.Sprintf("user/custom/%s/", hash))
					if fileName != "" {
						fmt.Printf("    - %s\n", fileName)
					}
				}
			}
			fmt.Printf("\n")
		} else {
			fmt.Printf("Hash: %s\n", hash)
			fmt.Printf("  Path: %s\n", s3Path)
			fmt.Printf("  (Empty directory)\n\n")
		}
	}

	fmt.Printf("Note: The hash is generated by AppStream and doesn't directly map to usernames.\n")
	fmt.Printf("To identify users, check the files in each directory or correlate with active session times.\n")

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

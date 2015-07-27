package main

// Brian Williams
// July 24, 2015
//
// Connect to two Couchbase clusters, source and destination,
// and do some simple set()/get() tests.  Mainly to illustrate
// using the golang sdk.
//
// This was developed using:
// * go version go1.3.3 darwin/amd64
// * Couchbase Go SDK 1.0 Beta [0.9.0]
//
// References:
// * http://docs.couchbase.com/developer/go-beta/introduction.html
// * http://docs.couchbase.com/developer/go-beta/managing-connections.html
// * http://godoc.org/github.com/couchbase/gocb#Connect
// * http://godoc.org/github.com/couchbase/gocb#Bucket.Insert

import "gopkg.in/couchbaselabs/gocb.v0"
import "fmt"
import "time"
import "log"
import "math/rand"
import "strconv"

// Given a Couchbase URL this will connect to the cluster
// and return back a gocb.Cluster and also the amount of 
// time taken to connect
func timedConnect(couchbaseUrl string) (*gocb.Cluster, error, int64) {

	startTime := time.Now().UnixNano()
	cluster, err := gocb.Connect(couchbaseUrl)
	endTime := time.Now().UnixNano()
	elapsedTime := (endTime - startTime)

	return cluster, err, elapsedTime
}

func main() {

	fmt.Println("Welcome to GolangReplicationLatencyTest")

	// Assume there is a one-way XDCR replication between the
	// source and destination buckets.  This could also be the
	// same cluster for testing purposes.

	sourceClusterURL := "couchbase://10.4.2.121"
	sourceBucketName := "BUCKETNAME"
	destinationClusterURL := "couchbase://10.4.2.122"
	destinationBucketName := "default"

	// First connect to the source and destination clusters
	srcCluster := couchbaseConnect(sourceClusterURL)
	destCluster := couchbaseConnect(destinationClusterURL)

	// Then connect to the source and destination buckets
	sourceBucket, _ := bucketConnect(srcCluster, sourceBucketName)
	destinationBucket, _ := bucketConnect(destCluster, destinationBucketName)

	var keepPollingGet bool
	var keepRunningTest bool
	keepRunningTest = true

	for keepRunningTest {

		// Write a value into sourceBucket

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		randomNumber := rng.Int31()

		// The document ID contains a random number
		documentName := "GoDocument" + strconv.Itoa(int(randomNumber))
		documentBody := "Hello there"
		var documentExpiration uint32
		documentExpiration = 0

		// fmt.Printf("Inserting document with key: %s\n", documentName);
		_, err := sourceBucket.Insert(documentName, &documentBody, documentExpiration)

		if err != nil {
			fmt.Println("Error inserting to bucket")
			log.Fatal(err)
		}

		// Poll destinationBucket to see when it arrives

		var returnedString string
		var elapsedGetTime int64
		keepPollingGet = true
		var errorHappened int = 0

		for keepPollingGet {

			startGetTime := time.Now().UnixNano()
			_, err2 := destinationBucket.Get(documentName, &returnedString)
			afterGetTime := time.Now().UnixNano()
			elapsedGetTime = afterGetTime - startGetTime

			if err2 != nil {
				fmt.Print(".")
				//fmt.Println(err2)
				errorHappened++
			} else {
				// fmt.Printf("The returned string is: %s\n", returnedString)
				keepPollingGet = false
			}

		} // keepPollingGet

		fmt.Println()

		fmt.Printf("Elapsed time to get %s:    %d ms  ( errors: %d )\n", documentName, 
			elapsedGetTime/1000000, errorHappened)
		// fmt.Printf("Errors: %d \n", errorHappened)

	} // keepRunningTest

	fmt.Println("Exiting...Goodbye")

	_ = srcCluster
	_ = sourceBucketName
	_ = sourceBucket
	_ = destCluster
	_ = destinationBucketName
	_ = destinationBucket
}

// Given a Cluster, and the name of a bucket, connect and return the
// Bucket, and the elapsed time to do so.
func bucketConnect(cluster *gocb.Cluster, bucketname string) (*gocb.Bucket, int64) {

	startTime := time.Now().UnixNano()

	bucket, err := cluster.OpenBucket(bucketname, "")

	endTime := time.Now().UnixNano()
	elapsedTime := (endTime - startTime)

	if err != nil {
		fmt.Printf("Error connecting to bucket %s", bucketname)
		log.Fatal(err)
	}

	return bucket, elapsedTime

}

// Connect to Couchbase cluster
func couchbaseConnect(cburl string) *gocb.Cluster {

	cluster, err, elapsedTimeMS := timedConnect(cburl)

	if err != nil {
		fmt.Printf("Error connecting to Couchbase")
		log.Fatal(err)
	} else {

		fmt.Printf("Cluster connect time:  %d \n", elapsedTimeMS/1000000)

	}

	return cluster
}

// EOF

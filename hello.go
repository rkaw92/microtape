package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

const maxGroupCommit = 10
const testIterations = 10
const numWriters = 1
const tickerIntervalMS = 100

const lastTestID = maxGroupCommit * testIterations

func main() {
	file, err := os.OpenFile("/run/microtape/data.tape", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}

	jobs := make(chan WriteJob, 1000)
	results := make(chan WriteResult, 1000)
	commits := make(chan CommittedWrite, 10)
	lastAnnouncedID := uint64(0)
	initialOffset := int64(0)

	commitTicker := time.NewTicker(tickerIntervalMS * time.Millisecond)

	for i := 0; i < numWriters; i++ {
		go writer(file, jobs, results)
	}

	// There's always just 1 aggregator
	go aggregator(file, lastAnnouncedID, commitTicker.C, results, commits)

	submitter := WriteSubmitter{lastAnnouncedID, initialOffset, file, jobs, results}
	finished := make(chan bool)
	lastJobChannel := make(chan WriteJob)

	go func() {
		var lastJob WriteJob
		for testIteration := uint64(0); testIteration < testIterations; testIteration++ {
			for i := uint64(0); i < maxGroupCommit; i++ {
				var data = []byte(fmt.Sprint("Hello, world ", testIteration*maxGroupCommit+i))
				lastJob = submitter.submit(data)
			}
		}
		lastJobChannel <- lastJob
	}()
	go waitForWrite(commits, lastTestID)
	go func() {
		lastJob := <-lastJobChannel
		success := <-lastJob.callback
		if success {
			fmt.Println("success for last write job:", lastJob.id)
		} else {
			fmt.Println("last job reports an error", lastJob.id)
		}
		finished <- true
	}()

	<-finished

	close(results)
	commitTicker.Stop()

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

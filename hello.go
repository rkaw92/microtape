package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"math"
	"os"
	"time"
)

const maxGroupCommit = 10000
const testIterations = 100
const numWriters = 1
const tickerIntervalMS = 100

const lastTestID = maxGroupCommit * testIterations

// A WriteJob specifies data that should be written to disk
type WriteJob struct {
	data     []byte
	offset   int64
	id       uint64
	callback chan bool
}

// A WriteResult describes data that was written to disk
type WriteResult struct {
	err      error
	offset   int64
	id       uint64
	callback chan bool
}

// A CommittedWrite informs about a write that was fsync()ed successfully
type CommittedWrite struct {
	offset int64
	id     uint64
}

func writer(file *os.File, jobs chan WriteJob, results chan WriteResult) {
	for job := range jobs {
		// TODO: Add internal error handling + retries
		//fmt.Println("job:", job)
		_, err := file.WriteAt(job.data, job.offset)
		results <- WriteResult{err, job.offset, job.id, job.callback}
	}
}

func aggregator(file *os.File, lastAnnouncedID uint64, ticker <-chan time.Time, results chan WriteResult, commits chan CommittedWrite) {
	completedWrites := make([]WriteResult, 0, maxGroupCommit)
	var min WriteResult = WriteResult{nil, int64(0), math.MaxUint64, nil}
	var max WriteResult = WriteResult{nil, int64(0), uint64(0), nil}

	for {
	singleRun:
		for {
			select {
			case result, ok := <-results:
				if !ok {
					break singleRun
				}
				if result.err != nil {
					panic(result.err)
				}
				completedWrites = append(completedWrites, result)
				if result.id > max.id {
					max = result
				}
				if result.id < min.id {
					min = result
				}
				if len(completedWrites) >= maxGroupCommit {
					break singleRun
				}
			case <-ticker:
				break singleRun
			}
		}

		// Group commit:
		isReady := (min.id == lastAnnouncedID+1 && (max.id-min.id+1) == uint64(len(completedWrites)))
		if isReady {
			newLatestWrite := max
			fmt.Println("ready - will sync at", newLatestWrite, "group width is", len(completedWrites))
			err := file.Sync()
			if err != nil {
				panic(err)
			}
			commits <- CommittedWrite{newLatestWrite.offset, newLatestWrite.id}
			for _, write := range completedWrites {
				write.callback <- true
				close(write.callback)
			}
			completedWrites = make([]WriteResult, 0, maxGroupCommit)
			lastAnnouncedID = newLatestWrite.id
			min = WriteResult{nil, int64(0), math.MaxUint64, nil}
			max = WriteResult{nil, int64(0), uint64(0), nil}
		}
	}
}

// A WriteSubmitter assigns sequential numbers to writes and submits them to the writer threads.
type WriteSubmitter struct {
	lastID  uint64
	offset  int64
	file    *os.File
	jobs    chan WriteJob
	results chan WriteResult
}

func (s *WriteSubmitter) submit(blob []byte) WriteJob {
	newID := s.lastID + 1
	job := WriteJob{blob, s.offset, newID, make(chan bool, 1)}
	s.jobs <- job
	s.lastID = newID
	s.offset += int64(len(blob))
	return job
}

func waitForWrite(commits chan CommittedWrite, awaitedID uint64) {
	for commit := range commits {
		if commit.id == awaitedID {
			break
		}
	}
}

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
			var data4KB []byte = make([]byte, 4096)
			rand.Read(data4KB)
			for i := uint64(0); i < maxGroupCommit; i++ {
				lastJob = submitter.submit(data4KB)
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

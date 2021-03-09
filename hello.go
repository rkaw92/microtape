package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

const maxGroupCommit = 2000

// A WriteJob specifies data that should be written to disk
type WriteJob struct {
	data   []byte
	offset int64
	id     uint64
}

// A WriteResult describes data that was written to disk
type WriteResult struct {
	err    error
	offset int64
	id     uint64
}

// A CommittedWrite informs about a write that was fsync()ed successfully
type CommittedWrite struct {
	offset int64
	id     uint64
}

func writer(file *os.File, job *WriteJob, results chan WriteResult) {
	// TODO: Add internal error handling + retries
	//fmt.Println("job:", job)
	_, err := file.WriteAt(job.data, job.offset)
	results <- WriteResult{err, job.offset, job.id}
}

func minmax(writes []WriteResult) (WriteResult, WriteResult) {
	var min WriteResult = WriteResult{nil, int64(0), math.MaxUint64}
	var max WriteResult = WriteResult{nil, int64(0), uint64(0)}
	for _, write := range writes {
		if write.id > max.id {
			max = write
		}
		if write.id < min.id {
			min = write
		}
	}
	return min, max
}

func getLatestWrite(last uint64, writes []WriteResult) (bool, WriteResult) {
	min, max := minmax(writes)
	isContiguous := uint64(len(writes)) == max.id-min.id+1
	return (min.id == last+1 && isContiguous), max
}

func aggregator(file *os.File, lastAnnouncedID uint64, ticker <-chan time.Time, results chan WriteResult, commits chan CommittedWrite) {
	completedWrites := make([]WriteResult, 0, maxGroupCommit)

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
				if len(completedWrites) >= maxGroupCommit {
					break singleRun
				}
			case <-ticker:
				break singleRun
			}
		}

		isReady, newLatestWrite := getLatestWrite(lastAnnouncedID, completedWrites)
		if isReady {
			fmt.Println("ready - will sync at", newLatestWrite, "group width is", len(completedWrites))
			err := file.Sync()
			if err != nil {
				panic(err)
			}
			commits <- CommittedWrite{newLatestWrite.offset, newLatestWrite.id}
			completedWrites = make([]WriteResult, 0, maxGroupCommit)
			lastAnnouncedID = newLatestWrite.id
		}
	}
}

// A WriteSubmitter assigns sequential numbers to writes and submits them to the writer threads.
type WriteSubmitter struct {
	lastID  uint64
	offset  int64
	file    *os.File
	results chan WriteResult
}

func (s *WriteSubmitter) submit(blob []byte) WriteJob {
	newID := s.lastID + 1
	job := WriteJob{blob, s.offset, newID}
	go writer(s.file, &job, s.results)
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

	results := make(chan WriteResult)
	commits := make(chan CommittedWrite)
	lastAnnouncedID := uint64(0)
	initialOffset := int64(0)

	commitTicker := time.NewTicker(100 * time.Millisecond)

	go aggregator(file, lastAnnouncedID, commitTicker.C, results, commits)
	submitter := WriteSubmitter{lastAnnouncedID, initialOffset, file, results}

	for testIteration := uint64(0); testIteration < 1000; testIteration++ {
		var jobID uint64 = 0
		for i := uint64(0); i < maxGroupCommit/2; i++ {
			submitter.submit([]byte("Hello "))
			jobID = submitter.submit([]byte("World " + strconv.FormatUint(testIteration*maxGroupCommit/2+i, 10) + "!")).id
		}

		waitForWrite(commits, jobID)
	}

	close(results)
	commitTicker.Stop()

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

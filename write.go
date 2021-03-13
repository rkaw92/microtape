package main

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/rkaw92/microtape/tar"
)

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
		dataLength := len(job.data)
		header := tar.MakeHeader(job.id, dataLength)
		_, err := file.WriteAt(header, job.offset)
		if err == nil {
			_, err = file.WriteAt(job.data, job.offset+tar.HeaderBytes)
			blockPaddingLength := (tar.HeaderBytes - (dataLength % tar.HeaderBytes)) & (tar.HeaderBytes - 1)
			blockPadding := make([]byte, blockPaddingLength)
			if blockPaddingLength > 0 {
				file.WriteAt(blockPadding, job.offset+tar.HeaderBytes+int64(dataLength))
			}
		}
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
	dataLength := len(blob)
	job := WriteJob{blob, s.offset, newID, make(chan bool, 1)}
	s.jobs <- job
	s.lastID = newID
	blockPaddingLength := (tar.HeaderBytes - (dataLength % tar.HeaderBytes)) & (tar.HeaderBytes - 1)
	totalSize := tar.HeaderBytes + dataLength + blockPaddingLength
	s.offset += int64(totalSize)
	return job
}

func waitForWrite(commits chan CommittedWrite, awaitedID uint64) {
	for commit := range commits {
		if commit.id == awaitedID {
			break
		}
	}
}

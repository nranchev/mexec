package main

import (
	"os"
	"fmt"
	"time"
	"bufio"
	"strconv"
	"strings"
	"os/exec"
)

type Job struct {
	cmdRaw string
	command string
	commandArgs []string
	exitStatus error
	runTimeSeconds float64
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s jobfile numWorkers\n")
		os.Exit(1)
	}

	jobFilename := os.Args[1] // The file containing the commands to execute
	numWorkers := func(arg string)(ret int){ 
		r, err := strconv.ParseInt(arg, 10, 64)
		if err != nil {
			fmt.Printf("Error parsing 'numWorkers'=%s, should be int\n", arg)
			os.Exit(1)
		}
		return int(r)
	}(os.Args[2]) // Execute n many jobs at once
	exitOnError := false // Wait for all jobs to finish or return on first failure

	fmt.Printf("jobFilename: %s, %d workers, exitOnError: %t\n", 
		jobFilename, numWorkers, exitOnError)

	// Compute the job list
	jobFile, _ := os.Open(jobFilename)
	jobReader := bufio.NewReader(jobFile)
	jobs := getJobs(jobReader)
	fmt.Printf("%d jobs will be submitted for execution\n\n", len(jobs))

	inbound := make(chan Job, len(jobs))
	outbound := make(chan Job, 0)

	// Submit all the jobs
	fmt.Printf("Submitting for execution:\n")
	for i, job := range jobs {
		fmt.Printf("[%d/%d] Submitting job=%s\n", i+1, len(jobs), job)
		inbound <- job
	}
	fmt.Printf("\n\nCompleting:\n");

	// Start the needed number of workers
	for i := 0; i < numWorkers; i++ {
		go startWorker(inbound, outbound)
	}

	// Check the outbound channel for completed jobs
	for i := 0; i < len(jobs); i++ {
		job := <-outbound
		if job.exitStatus != nil {
			if exitOnError {
				os.Exit(-1)
			}
		}
		fmt.Printf("[%d/%d] job '%s' completed successfully=%t - (%f seconds)\n", i+1, len(jobs), 
			job.cmdRaw, job.exitStatus == nil, job.runTimeSeconds)
	}
}

func startWorker(inbound chan Job, outbound chan Job) {
	for {
		job := <-inbound
		startTime := time.Now()
		job.exitStatus = exec.Command(job.command, job.commandArgs...).Run()
		job.runTimeSeconds = time.Since(startTime).Seconds()
		outbound <- job
	}
}

func getJobs(reader *bufio.Reader) (jobs []Job) {
	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil || isPrefix {
			return
		}
		jobs = append(jobs, createJob(string(line)))
	}
	return
}

func createJob(cmd string) (job Job) {
	job.cmdRaw = cmd
	var commandParts []string
	lastStart := 0
	for i := 0; i < len(cmd); i++ {
		if cmd[i] == ' ' || i+1 == len(cmd) {
			part := strings.Trim(cmd[lastStart:i+1], "\" ")
			if part != "" {
				commandParts = append(commandParts, part)
			}
			lastStart = i
		}
	}
	job.command = commandParts[0]
	job.commandArgs = commandParts[1:]
	return job
}

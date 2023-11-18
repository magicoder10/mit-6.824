package main

import (
	"regexp"
)

var goroutinesPattern = regexp.MustCompile("goroutines:(\\d+)")
var runningTasksPattern = regexp.MustCompile("runningTasks=map\\[([a-zA-Z0-9.: ]+)]")

var taskCountPattern = regexp.MustCompile("([a-zA-Z0-9.]+:\\d+):(\\d+)")

var connectPattern = regexp.MustCompile("^connect\\(([0-9]+)\\)")
var disconnectPattern = regexp.MustCompile("^disconnect\\(([0-9]+)\\)")

var startPattern = regexp.MustCompile("^start1\\(([0-9]+)\\)")
var crashPattern = regexp.MustCompile("^crash1\\(([0-9]+)\\)")
var networkStatePattern = regexp.MustCompile("^([0-9]+)  (connect|disconnect)\\([0-9]+\\) map\\[([0-9: a-zA-Z]+)]$")
var serverConnectionStatePattern = regexp.MustCompile("([0-9]+):([a-zA-Z]+)")

var lockDurationPattern = regexp.MustCompile("lockDuration=([0-9.]+[a-z]+)")

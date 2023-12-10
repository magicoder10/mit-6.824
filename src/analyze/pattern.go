package main

import (
	"regexp"
)

var goroutinesPattern = regexp.MustCompile("goroutines:(\\d+)")
var runningTasksPattern = regexp.MustCompile("runningTasks=map\\[([a-zA-Z0-9.: ]+)]")

var taskCountPattern = regexp.MustCompile("([a-zA-Z0-9.]+:\\d+):(\\d+)")

var connectPattern = regexp.MustCompile("\\sconnect\\(([0-9]+)\\)")
var disconnectPattern = regexp.MustCompile("\\sdisconnect\\(([0-9]+)\\)")

var startPattern = regexp.MustCompile("\\sstart1\\(([0-9]+)\\)")
var crashPattern = regexp.MustCompile("\\scrash1\\(([0-9]+)\\)")
var networkStatePattern = regexp.MustCompile("^([0-9]+)  [0-9:.]+ (connect|disconnect)\\([0-9]+\\) map\\[([0-9: a-zA-Z]+)]$")
var serverConnectionStatePattern = regexp.MustCompile("([0-9]+):([a-zA-Z]+)")

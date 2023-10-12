package main

import (
	"fmt"
	"regexp"
	"strings"

	"6.5840/mr"
)

var pattern = regexp.MustCompile("Author")

//
// a grep application "plugin" for MapReduce.
//
// go build -buildmode=plugin grep.go
//

func Map(key string, value string) []mr.KeyValue {
	lines := strings.Split(value, "\n")
	var matchedLines []mr.KeyValue
	for index, line := range lines {
		if pattern.MatchString(line) {
			matchedLines = append(matchedLines, mr.KeyValue{
				Key:   key,
				Value: fmt.Sprintf("%v", index),
			})
		}
	}

	return matchedLines
}

func Reduce(key string, values []string) string {
	return strings.Join(values, ",")
}

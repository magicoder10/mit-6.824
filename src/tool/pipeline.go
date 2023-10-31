package main

import (
	"regexp"
)

type Pipeline []OrOperator

type OrOperator []*regexp.Regexp

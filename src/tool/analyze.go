package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type Analyze struct {
	Pipelines map[string]Pipeline
}

type Pipeline []OrOperator

type OrOperator []*regexp.Regexp

var configFilePath string
var inputFilePath string
var outputDir string

var outputFileExt string

var rootCmd = &cobra.Command{
	Use:   "analyze",
	Short: "analyze log",
}

var filterCmd = &cobra.Command{
	Use:   "filter",
	Short: "filtering lines",
	RunE: func(cmd *cobra.Command, args []string) error {
		return filterLines(configFilePath, inputFilePath, outputDir)
	},
}

func main() {
	rootCmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", "", "config file path")
	err := rootCmd.MarkPersistentFlagRequired("config")
	if err != nil {
		fmt.Println(err)
		return
	}

	filterCmd.PersistentFlags().StringVarP(&inputFilePath, "input", "i", "", "input file path")
	err = filterCmd.MarkPersistentFlagRequired("input")
	if err != nil {
		fmt.Println(err)
		return
	}

	filterCmd.PersistentFlags().StringVarP(&outputDir, "outputDir", "o", "", "output directory")
	err = filterCmd.MarkPersistentFlagRequired("outputDir")
	if err != nil {
		fmt.Println(err)
		return
	}

	filterCmd.PersistentFlags().StringVarP(&outputFileExt, "outputFileExt", "e", "txt", "output file extension")
	if err != nil {
		fmt.Println(err)
		return
	}

	rootCmd.AddCommand(filterCmd)
	err = rootCmd.Execute()
}

func filterLines(filterFilePath string, inputFilePath string, outputDir string) error {
	buf, err := os.ReadFile(filterFilePath)
	if err != nil {
		return err
	}

	analyze, err := parseConfig(buf)
	if err != nil {
		return err
	}

	if len(analyze.Pipelines) == 0 {
		return fmt.Errorf("no pipeline found")
	}

	err = os.RemoveAll(outputDir)
	if err != nil {
		return err
	}

	err = os.MkdirAll(outputDir, 0755)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for pipelineName, pipeline := range analyze.Pipelines {
		wg.Add(1)
		go func(pipelineName string, pipeline Pipeline) {
			defer wg.Done()
			defer fmt.Printf("pipeline (%s) done\n", pipelineName)
			outputFilePath := filepath.Join(outputDir, fmt.Sprintf("%s.%s", pipelineName, outputFileExt))
			outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Println(err)
				return
			}

			defer outputFile.Close()

			inputFile, err := os.Open(inputFilePath)
			if err != nil {
				fmt.Println(err)
				return
			}

			defer inputFile.Close()

			scanner := bufio.NewScanner(inputFile)
			lineNumber := 1
			for scanner.Scan() {
				line := scanner.Text()
				if isMatch(pipeline, line) {
					_, err = fmt.Fprintf(outputFile, "%d  %s\n", lineNumber, line)
					if err != nil {
						fmt.Println(err)
					}
				}

				lineNumber++
			}
		}(pipelineName, pipeline)
	}

	wg.Wait()
	return nil
}

func isMatch(pipeline []OrOperator, line string) bool {
	for _, filter := range pipeline {
		if !isFilterMatch(filter, line) {
			return false
		}
	}

	return true
}

func isFilterMatch(orOperator OrOperator, line string) bool {
	for _, pattern := range orOperator {
		if pattern.MatchString(line) {
			return true
		}
	}

	return false
}

func parseConfig(buf []byte) (Analyze, error) {
	var config struct {
		Pipelines map[string][]struct {
			OrFilter []string `yaml:"or"`
		} `yaml:"pipelines"`
	}

	err := yaml.Unmarshal(buf, &config)
	if err != nil {
		return Analyze{}, err
	}

	analyze := Analyze{
		Pipelines: map[string]Pipeline{},
	}

	for pipelineName, pipelineConfig := range config.Pipelines {
		pipeline := Pipeline{}

		for _, andFilterConfig := range pipelineConfig {
			var compiledPatterns []*regexp.Regexp
			for _, pattern := range andFilterConfig.OrFilter {
				compiledPattern, err := regexp.Compile(pattern)
				if err != nil {
					return Analyze{}, err
				}

				compiledPatterns = append(compiledPatterns, compiledPattern)
			}

			pipeline = append(pipeline, compiledPatterns)
		}

		analyze.Pipelines[pipelineName] = pipeline
	}

	return analyze, nil
}

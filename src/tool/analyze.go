package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type Analyze struct {
	Pipelines map[string]Pipeline
}

var configFilePath string
var inputFilePath string
var outputDir string

var outputFilePath string

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

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "calculate stats",
	RunE: func(cmd *cobra.Command, args []string) error {
		return calcStats(inputFilePath, outputFilePath)
	},
}

var stateCmd = &cobra.Command{
	Use:   "state",
	Short: "calculate state",
	RunE: func(cmd *cobra.Command, args []string) error {
		return calcState(inputFilePath, outputFilePath)
	},
}

func main() {
	rootCmd.PersistentFlags().StringVarP(&inputFilePath, "input", "i", "", "input file path")
	err := rootCmd.MarkPersistentFlagRequired("input")
	if err != nil {
		fmt.Println(err)
		return
	}

	filterCmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", "", "config file path")
	err = filterCmd.MarkPersistentFlagRequired("config")
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

	statsCmd.PersistentFlags().StringVarP(&outputFilePath, "outputFilePath", "o", "", "output file path")
	err = statsCmd.MarkPersistentFlagRequired("outputFilePath")
	if err != nil {
		fmt.Println(err)
		return
	}

	stateCmd.PersistentFlags().StringVarP(&outputFilePath, "outputFilePath", "o", "", "output file path")
	err = stateCmd.MarkPersistentFlagRequired("outputFilePath")
	if err != nil {
		fmt.Println(err)
		return
	}

	rootCmd.AddCommand(filterCmd)
	rootCmd.AddCommand(statsCmd)
	rootCmd.AddCommand(stateCmd)
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

func calcStats(inputFilePath string, outputFilePath string) error {
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return err
	}

	defer inputFile.Close()

	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer outputFile.Close()

	stats := Stats{
		MaxGoroutines: 0,
		MaxTaskCount:  map[string]int{},
	}
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		includeLine(line, &stats)
	}

	buf, err := json.MarshalIndent(stats, "", strings.Repeat(" ", 4))
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(outputFile, "%s\n", buf)
	return err
}

func includeLine(line string, stats *Stats) {
	goroutineCounter, match, err := parseGoroutines(line)
	if err != nil {
		fmt.Println(err)
	}

	if match {
		stats.MaxGoroutines = max(goroutineCounter.Count, stats.MaxGoroutines)
	}

	runningTasks, match, err := parseRunningTasks(line)
	if err != nil {
		fmt.Println(err)
	}

	if match {
		for taskName, count := range runningTasks.TaskCount {
			if _, ok := stats.MaxTaskCount[taskName]; !ok {
				stats.MaxTaskCount[taskName] = count
			} else {
				stats.MaxTaskCount[taskName] = max(count, stats.MaxTaskCount[taskName])
			}
		}
	}
}

func parseGoroutines(line string) (Goroutines, bool, error) {
	groups := goroutinesPattern.FindStringSubmatch(line)
	if len(groups) != 2 {
		return Goroutines{}, false, nil
	}

	count, err := strconv.ParseInt(groups[1], 10, 64)
	if err != nil {
		return Goroutines{}, false, err
	}

	return Goroutines{
		Count: int(count),
	}, true, nil
}

func parseRunningTasks(line string) (RunningTasks, bool, error) {
	groups := runningTasksPattern.FindStringSubmatch(line)
	if len(groups) != 2 {
		return RunningTasks{}, false, nil
	}

	taskCountMap := map[string]int{}
	taskCountInputs := strings.Split(groups[1], " ")
	for _, taskCountInput := range taskCountInputs {
		taskCount, err := parseTaskCount(taskCountInput)
		if err != nil {
			return RunningTasks{}, false, err
		}

		taskCountMap[taskCount.TaskName] = taskCount.Count
	}

	return RunningTasks{
		TaskCount: taskCountMap,
	}, true, nil
}

func parseTaskCount(input string) (TaskCount, error) {
	groups := taskCountPattern.FindStringSubmatch(input)
	if len(groups) != 3 {
		return TaskCount{}, fmt.Errorf("invalid input: %s", input)
	}

	count, err := strconv.ParseInt(groups[2], 10, 64)
	if err != nil {
		return TaskCount{}, err
	}

	return TaskCount{
		TaskName: groups[1],
		Count:    int(count),
	}, nil
}

func calcState(inputFilePath string, outputFilePath string) error {
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return err
	}

	defer inputFile.Close()

	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer outputFile.Close()

	state := State{
		Network: map[int]ConnectionState{},
	}

	lineNumber := 1
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		isUpdated := updateNetworkState(&state, line)
		if isUpdated {
			_, err = fmt.Fprintf(outputFile, "%d  %s state=%+v\n", lineNumber, line, state)
		}

		lineNumber++
	}

	return err
}

func updateNetworkState(state *State, line string) bool {
	connect, match, err := parseConnect(line)
	if err != nil {
		fmt.Println(err)
	}

	if match {
		state.Network[connect.ServerID] = ConnectedConnectionState
		return true
	}

	disconnect, match, err := parseDisconnect(line)
	if err != nil {
		fmt.Println(err)
	}

	if match {
		state.Network[disconnect.ServerID] = DisconnectedConnectionState
		return true
	}

	return false
}

func parseConnect(input string) (Connect, bool, error) {
	groups := connectPattern.FindStringSubmatch(input)
	if len(groups) != 2 {
		return Connect{}, false, nil
	}

	serverID, err := strconv.ParseInt(groups[1], 10, 64)
	if err != nil {
		return Connect{}, false, err
	}

	return Connect{
		ServerID: int(serverID),
	}, true, nil
}

func parseDisconnect(input string) (Disconnect, bool, error) {
	groups := disconnectPattern.FindStringSubmatch(input)
	if len(groups) != 2 {
		return Disconnect{}, false, nil
	}

	serverID, err := strconv.ParseInt(groups[1], 10, 64)
	if err != nil {
		return Disconnect{}, false, err
	}

	return Disconnect{
		ServerID: int(serverID),
	}, true, nil
}

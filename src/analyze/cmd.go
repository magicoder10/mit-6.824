package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

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
var logLineNumber int

var rootCmd = &cobra.Command{
	Use:   "analyze",
	Short: "analyze log",
}

var allCmd = &cobra.Command{
	Use:   "all",
	Short: "perform all analysis",
	RunE: func(cmd *cobra.Command, args []string) error {
		os.RemoveAll(outputDir)

		err := filterLines(configFilePath, inputFilePath, path.Join(outputDir, "log"), "log")
		if err != nil {
			return err
		}

		err = calcStats(inputFilePath, filepath.Join(outputDir, "stats.txt"))
		if err != nil {
			return err
		}

		err = calcState(inputFilePath, filepath.Join(outputDir, "state"))
		if err != nil {
			return err
		}

		return nil
	},
}

var filterCmd = &cobra.Command{
	Use:   "filter",
	Short: "filtering lines",
	RunE: func(cmd *cobra.Command, args []string) error {
		return filterLines(configFilePath, inputFilePath, outputDir, outputFileExt)
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
		return calcState(inputFilePath, outputDir)
	},
}

var queryNetworkStateCmd = &cobra.Command{
	Use: "network",
	RunE: func(cmd *cobra.Command, args []string) error {
		return getNetworkState(inputFilePath, logLineNumber)
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

	stateCmd.PersistentFlags().StringVarP(&outputDir, "outputDir", "o", "", "output directory")
	if err != nil {
		fmt.Println(err)
		return
	}

	queryNetworkStateCmd.PersistentFlags().IntVarP(&logLineNumber, "logLine", "l", 0, "log line number")
	err = queryNetworkStateCmd.MarkPersistentFlagRequired("logLine")
	if err != nil {
		fmt.Println(err)
		return
	}

	stateCmd.AddCommand(queryNetworkStateCmd)

	allCmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", "", "config file path")
	err = allCmd.MarkPersistentFlagRequired("config")
	if err != nil {
		fmt.Println(err)
		return
	}

	allCmd.PersistentFlags().StringVarP(&outputDir, "outputDir", "o", "", "output directory")
	err = allCmd.MarkPersistentFlagRequired("outputDir")
	if err != nil {
		fmt.Println(err)
		return
	}

	rootCmd.AddCommand(filterCmd)
	rootCmd.AddCommand(statsCmd)
	rootCmd.AddCommand(stateCmd)
	rootCmd.AddCommand(allCmd)
	err = rootCmd.Execute()
}

func filterLines(filterFilePath string, inputFilePath string, outputDir string, outputFileExt string) error {
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
			pipelineOutputFilePath := filepath.Join(outputDir, fmt.Sprintf("%s.%s", pipelineName, outputFileExt))
			outputFile, err := os.OpenFile(pipelineOutputFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
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
		MaxGoroutines:     0,
		MaxTaskCount:      map[string]int{},
		TotalLockDuration: time.Duration(0),
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
	if err == nil {
		fmt.Println("generated stats")
	}

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

	lockDuration, match, err := parseLockDuration(line)
	if err != nil {
		fmt.Println(err)
	}

	if match {
		stats.TotalLockDuration += lockDuration
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

func parseLockDuration(line string) (time.Duration, bool, error) {
	groups := lockDurationPattern.FindStringSubmatch(line)
	if len(groups) != 2 {
		return 0, false, nil
	}

	duration, err := time.ParseDuration(groups[1])
	if err != nil {
		return 0, false, err
	}

	return duration, true, nil
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

func calcState(inputFilePath string, outputDir string) error {
	if len(outputDir) == 0 {
		return fmt.Errorf("output directory is empty")
	}

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return err
	}

	defer inputFile.Close()

	err = os.MkdirAll(outputDir, 0755)
	if err != nil {
		return err
	}

	networkOutputFile, err := os.OpenFile(filepath.Join(outputDir, "network.txt"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer networkOutputFile.Close()

	serversOutputFile, err := os.OpenFile(filepath.Join(outputDir, "servers.txt"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer serversOutputFile.Close()

	state := newState()
	lineNum := 1
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		isUpdated := updateNetworkState(&state, line)
		if isUpdated {
			_, err = fmt.Fprintf(networkOutputFile, "%d  %s %+v\n", lineNum, line, state.Network)
		}

		isUpdated = updateServerState(&state, line)
		if isUpdated {
			_, err = fmt.Fprintf(serversOutputFile, "%d  %s %+v\n", lineNum, line, state.Servers)
		}

		lineNum++
	}

	if err == nil {
		fmt.Println("generated state")
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

func updateServerState(state *State, line string) bool {
	start, match, err := parseStart(line)
	if err != nil {
		fmt.Println(err)
	}

	if match {
		state.Servers[start.ServerID] = UpServerState
		return true
	}

	crash, match, err := parseCrash(line)
	if err != nil {
		fmt.Println(err)
	}

	if match {
		state.Servers[crash.ServerID] = DownServerState
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

func parseStart(input string) (Start, bool, error) {
	groups := startPattern.FindStringSubmatch(input)
	if len(groups) != 2 {
		return Start{}, false, nil
	}

	serverID, err := strconv.ParseInt(groups[1], 10, 64)
	if err != nil {
		return Start{}, false, err
	}

	return Start{
		ServerID: int(serverID),
	}, true, nil
}

func parseCrash(input string) (Crash, bool, error) {
	groups := crashPattern.FindStringSubmatch(input)
	if len(groups) != 2 {
		return Crash{}, false, nil
	}

	serverID, err := strconv.ParseInt(groups[1], 10, 64)
	if err != nil {
		return Crash{}, false, err
	}

	return Crash{
		ServerID: int(serverID),
	}, true, nil
}

func getNetworkState(inputFilePath string, logLineNumber int) error {
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	defer inputFile.Close()

	var prevNetworkState map[int]ConnectionState
	lineNum := 1
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()

		logLineNum, networkState, err := parseNetworkState(line)
		if err != nil {
			fmt.Println(err)
			return nil
		}

		if logLineNum > logLineNumber {
			break
		}

		prevNetworkState = networkState
		lineNum++
	}

	if prevNetworkState == nil {
		fmt.Printf("no network state at line %v\n", logLineNumber)
		return nil
	}

	fmt.Printf("%+v\n", prevNetworkState)
	return nil
}

func parseNetworkState(input string) (int, map[int]ConnectionState, error) {
	groups := networkStatePattern.FindStringSubmatch(input)
	if len(groups) != 4 {
		return 0, nil, nil
	}

	logLine, err := strconv.ParseInt(groups[1], 0, 64)
	if err != nil {
		return 0, nil, err
	}

	networkStateLine := groups[3]
	serverConnectionStates := strings.Split(networkStateLine, " ")

	networkState := make(map[int]ConnectionState)
	for _, serverConnectionState := range serverConnectionStates {
		serverConnectionGroups := serverConnectionStatePattern.FindStringSubmatch(serverConnectionState)
		if len(serverConnectionGroups) != 3 {
			return 0, nil, nil
		}

		serverID, err := strconv.ParseInt(serverConnectionGroups[1], 10, 64)
		if err != nil {
			return 0, nil, err
		}

		networkState[int(serverID)] = ConnectionState(serverConnectionGroups[2])
	}

	return int(logLine), networkState, nil
}

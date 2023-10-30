# Analyze
Enable everyone to debug and finish 6.824 labs with ease.

## Installation
```bash
go install analyze.go
```

## Usage
### Filter logs

Create `.analyze.yaml` under the root directory of your lab, and add the following content:
```yaml
pipelines:
  test:
    - or:
        - start[0-9]+\([0-9]+\)
        - crash[0-9]+\([0-9]+\)
        - connect\([0-9]+\)
        - disconnect\([0-9]+\)
        - one\([0-9]+\)
        - Start\([0-9]+\)
        - \[Test\]
        - Passed
        - 'panic: test timed out after'
  kill:
    - or:
        - Kill enter
        - Kill exit
        - flow:Termination
        - 'panic: test timed out after'
  server-3-term-84:
    - or:
        - serverID:3
    - or:
        - term:84
    - or:
        - flow:LogReplication
```

`.analyze.yaml` contains a list of pipelines. Each pipeline contains a list of filters.
Filters are connected by `and` in a pipeline. `or` filter connect regular expressions 
with `or` operator.

Run the following command to filter logs:
```bash
analyze filter -c .analyze.yaml -o logs/analyze/output -e log -i logs/analyze/TestReElection2A_7034.log
```

### View system statistics

#### Prerequisite
Please use `logging.go` inside `raft` package for logging.

Run the following command to view system statistics:
```bash
analyze stats -o raft/logs/analyze/stats.txt -i raft/logs/running/TestFigure8Unreliable2C_3.log
```

Sample output:
```
{
    "MaxGoroutines": 690,
    "MaxTaskCount": {
        "raft.go:1008": 4,
        "raft.go:1651": 1,
        "raft.go:522": 0,
        "raft.go:588": 1,
        "raft.go:648": 0,
        "raft.go:671": 1,
        "raft.go:681": 1,
        "raft.go:691": 1,
        "raft.go:727": 4,
        "raft.go:738": 18,
        "raft.go:920": 106
    }
}
```

### View system state
Please set `printConfigDebugLog` to true inside `config.go` before running the test.

Run the following command to view system state:
```bash
analyze stats -o raft/logs/analyze/stats.txt -i raft/logs/running/TestFigure8Unreliable2C_3.log
```

Sample output:
```
58148  disconnect(2) state={Network:map[0:Connected 1:Disconnected 2:Disconnected 3:Disconnected 4:Connected]}
58149  connect(2) state={Network:map[0:Connected 1:Disconnected 2:Connected 3:Disconnected 4:Connected]}
58216  disconnect(2) state={Network:map[0:Connected 1:Disconnected 2:Disconnected 3:Disconnected 4:Connected]}
58217  connect(3) state={Network:map[0:Connected 1:Disconnected 2:Disconnected 3:Connected 4:Connected]}
59427  disconnect(3) state={Network:map[0:Connected 1:Disconnected 2:Disconnected 3:Disconnected 4:Connected]}
59428  connect(1) state={Network:map[0:Connected 1:Connected 2:Disconnected 3:Disconnected 4:Connected]}
59537  disconnect(1) state={Network:map[0:Connected 1:Disconnected 2:Disconnected 3:Disconnected 4:Connected]}
59538  connect(2) state={Network:map[0:Connected 1:Disconnected 2:Connected 3:Disconnected 4:Connected]}
59650  connect(1) state={Network:map[0:Connected 1:Connected 2:Connected 3:Disconnected 4:Connected]}
59651  connect(3) state={Network:map[0:Connected 1:Connected 2:Connected 3:Connected 4:Connected]}
```

## Community
If you find any bugs or want to add new features, please feel free to open an issue or submit a pull request.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	f, err := os.Open("etcd.metadata")
	if err != nil {
		panic("failed to open metadata file, " + err.Error())
	}
	reader := bufio.NewReader(f)
	eventbus := map[string]map[string]interface{}{}
	subscriptions := map[string]map[string]interface{}{}
	for {
		k, err := readline(reader)
		if err == io.EOF {
			break
		}
		key := string(k)
		value, err := readline(reader)
		if err != nil {
			panic("corrupted data, " + string(key))
		}
		m := map[string]interface{}{}
		if strings.HasPrefix(key, "/vanus/vanus/internal/resource/eventbus") {
			if err = json.Unmarshal(value, &m); err != nil {
				panic(fmt.Sprintf("failed to unmarshall data, key: %s, valus: %s", key, string(value)))
			}
			eventbus[key] = m
		} else if strings.HasPrefix(key, "/vanus/trigger/subscriptions") {
			if err = json.Unmarshal(value, &m); err != nil {
				panic(fmt.Sprintf("failed to unmarshall data, key: %s, valus: %s", key, string(value)))
			}
			subscriptions[key] = m
		}
	}
	_ = f.Close()
	f, err = os.Create("vanus-restore.sh")
	if err != nil {
		panic("failed to create script file, " + err.Error())
	}
	generateEventbusCreateScripts(f, eventbus)
	generateSubscriptions(f, subscriptions)
	_ = f.Close()
}

func generateEventbusCreateScripts(w io.Writer, m map[string]map[string]interface{}) {
	for _, v := range m {
		name := v["name"].(string)
		logNum := v["log_number"].(float64)
		desc := v["description"]
		if !strings.HasPrefix(name, "__") {
			cmd := fmt.Sprintf("vsctl eventbus create --name %s --eventlog %d --description '%s'\n", name, int(logNum), desc)
			_, _ = w.Write([]byte(cmd))
		}
	}
}

func generateSubscriptions(w io.Writer, m map[string]map[string]interface{}) {
	for k, v := range m {
		data1, err := json.Marshal(v["filters"])
		if err != nil {
			panic(fmt.Sprintf("failed to marshall filters, key: %s, value: %v", k, v["filters"]))
		}
		data2, err := json.Marshal(v["transformer"])
		if err != nil {
			panic(fmt.Sprintf("failed to marshall transformer, key: %s, value: %v", k, v["transformer"]))
		}
		sink := v["sink"].(string)
		disable := v["phase"].(string) == "stopped"
		eventbus := v["eventbus"].(string)
		name := v["name"].(string)
		description := v["description"].(string)
		cmd := fmt.Sprintf("vsctl subscription create --name %s --eventbus %s --sink %s --description '%s' --disable %v --filters '%s' --transformer '%s'\n",
			name, eventbus, sink, description, disable, string(data1), string(data2))
		_, _ = w.Write([]byte(cmd))
	}
}

func readline(reader *bufio.Reader) ([]byte, error) {
	line, prefix, err := reader.ReadLine()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		panic("read file error, " + err.Error())
	}
	for prefix {
		var _line []byte
		_line, prefix, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic("read file error, " + err.Error())
		}
		line = append(line, _line...)
	}
	return line, nil
}

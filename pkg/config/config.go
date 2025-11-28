package config

import "errors"

var G Config

type Config struct {
	StateRootDir string
	Connectors   map[string]Connector
	Tasks        []Task
}

func GetConnector(name string) (Connector, error) {
	config, ok := G.Connectors[name]
	if !ok {
		return Connector{}, errors.New("can not found connector " + name)
	}
	return config, nil
}

func GetStateFileName(name string) string {
	return G.StateRootDir + "/" + name + ".sv"
}

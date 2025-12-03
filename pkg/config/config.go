package config

import (
	"errors"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var G Config

type BaseConfig struct {
	DataDir string `yaml:"data_dir"`
}

type Config struct {
	Base       BaseConfig
	Connectors map[string]Connector
	Tasks      []Task
}

func GetConnector(name string) (Connector, error) {
	config, ok := G.Connectors[name]
	if !ok {
		return Connector{}, errors.New("can not found connector " + name)
	}
	return config, nil
}

func GetStateFileName(name string) string {
	return path.Join(G.Base.DataDir, name+".sv")
}

func Parse(dir string) {
	var config BaseConfig
	if err := loadYaml(path.Join(dir, "config.yaml"), &config); err != nil {
		panic(err)
	}
	G.Base = config
	var connectors struct {
		Connectors []Connector `yaml:"connectors"`
	}

	if err := loadYaml(path.Join(dir, "connectors.yaml"), &connectors); err != nil {
		panic("Can not load connectors.yaml, reason: " + err.Error())
	}
	G.Connectors = make(map[string]Connector)
	for _, v := range connectors.Connectors {
		if _, ok := G.Connectors[v.Alias]; ok {
			panic("Duplicate connector: " + v.Alias)
		}
		G.Connectors[v.Alias] = v
	}
	_ = filepath.Walk(path.Join(dir, "tasks"), func(path string, info os.FileInfo, err error) error {
		var task Task
		if !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		if err := loadYaml(path, &task); err != nil {
			panic("Can not load " + (path) + ", reason: " + err.Error())
		}
		G.Tasks = append(G.Tasks, task)
		return nil
	})

}

func loadYaml(file string, v interface{}) error {
	log.Println("Starting loadYAML:" + file)
	content, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(content, v); err != nil {
		return err
	}
	return nil
}

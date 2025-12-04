package config

import (
	"errors"
	"github.com/imiskolee/anycdc/pkg/logs"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var G Config

const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
)

type Admin struct {
	Listen string `yaml:"listen;default=:9999"`
}

type BaseConfig struct {
	DataDir  string `yaml:"data_dir"`
	LogLevel string `yaml:"log_level;default=info"`
	Admin    Admin  `yaml:"admin"`
}

type Config struct {
	Base       BaseConfig
	Connectors map[string]Connector
	Tasks      []Task
}

func GetConnector(name string) (Connector, error) {
	config, ok := G.Connectors[name]
	if !ok {
		logs.Error("connector %s not found", name)
		return Connector{}, errors.New("can not found connector " + name)
	}
	return config, nil
}

func GetStateFileName(name string) string {
	return path.Join(G.Base.DataDir, name+".sv")
}

var errInvalidConfig = errors.New("INVALID_CONFIG")

func Parse(dir string) error {
	logs.Info("Starting parse config on dir:%s", dir)

	var config BaseConfig
	{
		p := path.Join(dir, "config.yaml")
		if err := loadYaml(p, &config); err != nil {
			logs.Error("Can not load %s, because of %s", p, err)
			return err
		}
	}
	G.Base = config
	var connectors struct {
		Connectors []Connector `yaml:"connectors"`
	}
	{
		p := path.Join(dir, "connectors.yaml")
		if err := loadYaml(p, &connectors); err != nil {
			logs.Error("Can not load %s, because of %s", p, err)
			return err
		}
	}

	G.Connectors = make(map[string]Connector)
	for k, v := range connectors.Connectors {
		if v.Alias == "" {
			logs.Error("empty connector alias on index: %d", k)
			return errInvalidConfig
		}
		if _, ok := G.Connectors[v.Alias]; ok {
			logs.Error("duplicate connectors, already define connector %s before.", v.Alias)
			return errInvalidConfig
		}
		G.Connectors[v.Alias] = v
	}
	taskMap := make(map[string]bool)
	_ = filepath.Walk(path.Join(dir, "tasks"), func(path string, info os.FileInfo, err error) error {
		var task Task
		if !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		if err := loadYaml(path, &task); err != nil {
			logs.Error("can not load task file:%s, because of %s", path, err)
			return nil
		}
		if _, ok := taskMap[task.Name]; ok {
			logs.Error("duplicate tasks, already define task %s before.", task.Name)
			return nil
		}
		task.Path = path
		taskMap[task.Name] = true
		G.Tasks = append(G.Tasks, task)
		return nil
	})
	return nil
}

func loadYaml(file string, v interface{}) error {
	logs.Info("Starting loadYAML:%s", file)
	content, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(content, v); err != nil {
		return err
	}
	return nil
}

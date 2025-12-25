package config

import (
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"os"
)

var G Config

const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
)

type Database struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type Admin struct {
	Listen   string   `yaml:"listen"`
	Database Database `yaml:"database"`
}

type Config struct {
	DataDir  string `yaml:"data_dir"`
	Tester   bool   `yaml:"tester"`
	LogLevel string `yaml:"log_level;default=info"`
	Admin    Admin  `yaml:"admin"`
}

func Parse(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		panic("can not read config file:" + err.Error())
	}
	if err := yaml.Unmarshal(data, &G); err != nil {
		panic("can not parse config file:" + err.Error())
	}
	return
}

func ParseFromReader(reader io.Reader) {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		panic("can not read config file:" + err.Error())
	}
	if err := yaml.Unmarshal(data, &G); err != nil {
		panic("can not parse config file:" + err.Error())
	}
	return
}

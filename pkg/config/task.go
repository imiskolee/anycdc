package config

type Reader struct {
	Connector string            `yaml:"connector"`
	Tables    []string          `yaml:"tables"`
	Extras    map[string]string `yaml:"extras"`
}

type Writer struct {
	Connector string `yaml:"connector"`
}

type Task struct {
	Reader    Reader   `yaml:"reader"`
	Writers   []Writer `yaml:"writers"`
	Tables    []string `yaml:"tables"`
	Interval  int      `yaml:"interval"`
	QueueSize int      `yaml:"queue_size"`
}

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
	Path    string   `yaml:"-"`
	Name    string   `yaml:"name"`
	Reader  Reader   `yaml:"reader"`
	Writers []Writer `yaml:"writers"`
}

func (s Task) Reload() error {
	if err := loadYaml(s.Path, &s); err != nil {
		return err
	}
	return nil
}

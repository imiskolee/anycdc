package state

import (
	"github.com/imiskolee/anycdc/pkg/config"
	"os"
)

type State struct {
	name string
}

func NewState(name string) *State {
	return &State{name: name}
}

func (s *State) Load() string {
	fname := s.getFileName()
	state, err := os.ReadFile(fname)
	if os.IsNotExist(err) {
		return ""
	}
	return string(state)
}

func (s *State) Save(state string) error {
	fname := s.getFileName()
	return os.WriteFile(fname, []byte(state), 0644)
}

func (s *State) Clear() error {
	fname := s.getFileName()
	_ = os.Remove(fname)
	return nil
}

func (s *State) getFileName() string {
	return config.GetStateFileName(s.name)
}

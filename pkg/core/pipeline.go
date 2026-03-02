package core

import (
	"sync"
	"time"
)

type Pipeline struct {
	CreatedAt   time.Time
	Events      map[string][]Event
	mutex       sync.Mutex
	cdcPosition string
	Count       int
}

func (s *Pipeline) Append(cdcPosition string, event Event) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.cdcPosition = cdcPosition
	if _, ok := s.Events[event.SourceSchema.Name]; !ok {
		s.Events[event.SourceSchema.Name] = []Event{}
	}
	s.Events[event.SourceSchema.Name] = append(s.Events[event.SourceSchema.Name], event)
	s.Count++
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		CreatedAt: time.Now(),
		Events:    make(map[string][]Event),
	}
}

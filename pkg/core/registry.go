package core

import "context"

type Registry[T any] struct {
	factories map[string]func(ctx context.Context, opt interface{}) T
}

func NewRegistry[T any]() *Registry[T] {
	return &Registry[T]{
		factories: make(map[string]func(ctx context.Context, opt interface{}) T),
	}
}

func (s *Registry[T]) Register(name string, factory func(ctx context.Context, opt interface{}) T) {
	s.factories[name] = factory
}

func (s *Registry[T]) Get(name string) func(ctx context.Context, opt interface{}) T {
	return s.factories[name]
}

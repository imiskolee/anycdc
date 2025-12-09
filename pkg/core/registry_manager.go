package core

type RegistryManager struct {
	Reader *Registry[Reader]
	Writer *Registry[Writer]
	Schema *Registry[SchemaManager]
}

var Registries RegistryManager

func init() {
	Registries.Reader = NewRegistry[Reader]()
	Registries.Writer = NewRegistry[Writer]()
	Registries.Schema = NewRegistry[SchemaManager]()
}

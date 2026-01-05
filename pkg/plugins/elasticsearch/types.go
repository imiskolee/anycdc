package elasticsearch

import (
	"github.com/imiskolee/anycdc/pkg/core/schemas"
	"github.com/imiskolee/anycdc/pkg/core/types"
)

var typMap *types.Map = types.NewMap()

func init() {
	typMap.RegisterDecoder(schemas.TypeJSON, jsonDecode)
}

func jsonDecode(v interface{}) (interface{}, error) {
	return v, nil
}

package reader

import "bindolabs/anycdc/pkg/config"

var _readers map[config.ConnectorType]func(conf config.Reader, opt *ReaderOptions) Reader

func init() {
	_readers = map[config.ConnectorType]func(conf config.Reader, opt *ReaderOptions) Reader{}
}
func Register(connectorType config.ConnectorType, reader func(conf config.Reader, opt *ReaderOptions) Reader) {
	_readers[connectorType] = reader
}

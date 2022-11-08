package structmap

import (
	"fmt"

	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
)

type AssetWrapper struct {
	A *v1beta2.Asset
}

func (w AssetWrapper) AsMap() (map[string]interface{}, error) {
	v, err := AsMap(w.A)
	if err != nil {
		return nil, fmt.Errorf("structmap: asset as map: %w", err)
	}

	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("structmap: asset as map: unexpected type for asset map: %T", v)
	}

	return m, err
}

func (w *AssetWrapper) OverwriteWith(m map[string]interface{}) error {
	dataMap, ok := m["data"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("structmap: overwrite asset: unexpected type for asset data: %T", m["data"])
	}

	mt, err := protoregistry.GlobalTypes.FindMessageByName(w.A.Data.MessageName())
	if err != nil {
		return fmt.Errorf("structmap: overwrite asset: resolve type by full name %s: %w", w.A.Data.MessageName(), err)
	}

	msg := mt.New().Interface()
	delete(dataMap, "@type")
	if err := AsStruct(m["data"], &msg); err != nil {
		return fmt.Errorf("structmap: overwrite asset: decode asset data: %w", err)
	}

	delete(m, "data")
	if err := AsStruct(m, w.A); err != nil {
		return fmt.Errorf("structmap: overwrite asset: decode asset: %w", err)
	}

	data, err := anypb.New(msg)
	if err != nil {
		return fmt.Errorf("structmap: overwrite asset: marshal data as any: %w", err)
	}

	w.A.Data = data

	return nil
}

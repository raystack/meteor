package models

import (
	"fmt"

	assetsv1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"google.golang.org/protobuf/encoding/protojson"
)

func NewURN(service, scope, kind, id string) string {
	return fmt.Sprintf(
		"urn:%s:%s:%s:%s",
		service, scope, kind, id,
	)
}

func ToJSON(asset *assetsv1beta2.Asset) ([]byte, error) {
	return protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}.Marshal(asset)
}

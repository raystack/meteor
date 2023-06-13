package models_test

import (
	"testing"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/stretchr/testify/assert"
)

func TestNewRecord(t *testing.T) {
	type args struct {
		data *v1beta2.Asset
	}
	tests := []struct {
		name     string
		args     args
		expected models.Record
	}{
		{
			name: "should return a new record",
			args: args{
				data: &v1beta2.Asset{},
			},
			expected: models.NewRecord(&v1beta2.Asset{}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := models.NewRecord(tt.args.data)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestRecord_Data(t *testing.T) {
	type fields struct {
		data *v1beta2.Asset
	}
	tests := []struct {
		name     string
		fields   fields
		expected *v1beta2.Asset
	}{
		{
			name: "should return the record data",
			fields: fields{
				data: &v1beta2.Asset{
					Name: "test",
				},
			},
			expected: &v1beta2.Asset{
				Name: "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := models.NewRecord(tt.fields.data)
			actual := r.Data()
			assert.Equal(t, tt.expected, actual)
		})
	}
}

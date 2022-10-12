package utils

import (
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func StatusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	var se interface {
		GRPCStatus() *status.Status
	}
	if errors.As(err, &se) {
		return se.GRPCStatus().Code()
	}

	return codes.Unknown
}

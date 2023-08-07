package utils

import (
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var codeToStr = map[codes.Code]string{
	codes.OK:                 `"OK"`,
	codes.Canceled:           `"CANCELED"`,
	codes.Unknown:            `"UNKNOWN"`,
	codes.InvalidArgument:    `"INVALID_ARGUMENT"`,
	codes.DeadlineExceeded:   `"DEADLINE_EXCEEDED"`,
	codes.NotFound:           `"NOT_FOUND"`,
	codes.AlreadyExists:      `"ALREADY_EXISTS"`,
	codes.PermissionDenied:   `"PERMISSION_DENIED"`,
	codes.ResourceExhausted:  `"RESOURCE_EXHAUSTED"`,
	codes.FailedPrecondition: `"FAILED_PRECONDITION"`,
	codes.Aborted:            `"ABORTED"`,
	codes.OutOfRange:         `"OUT_OF_RANGE"`,
	codes.Unimplemented:      `"UNIMPLEMENTED"`,
	codes.Internal:           `"INTERNAL"`,
	codes.Unavailable:        `"UNAVAILABLE"`,
	codes.DataLoss:           `"DATA_LOSS"`,
	codes.Unauthenticated:    `"UNAUTHENTICATED"`,
}

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

func StatusText(err error) string {
	return codeToStr[StatusCode(err)]
}

/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"fmt"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/talos/thrift/errors"
)

type TalosErrorCodePeeker interface {
	GetErrorCode() common.ErrorCode
}

type TalosTransportError struct {
	HttpStatusCode errors.HttpStatusCode
	ErrorCode      errors.ErrorCode
	ErrorMessage   string
	ServerTime     int64
}

type TalosRuntimeError struct {
	ErrorCode common.ErrorCode
	error
}

func (e *TalosRuntimeError) GetErrorCode() common.ErrorCode {
	return e.ErrorCode
}

func (e *TalosRuntimeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Talos runtime error, ErrorCode: %s, ErrorMessage: %s",
		e.ErrorCode, e.error.Error())
}

func NewTalosTransportError(httpStatusCode errors.HttpStatusCode,
	errorMessage string, timestamp int64) *TalosTransportError {
	errorCode := errors.ErrorCode_UNKNOWN
	if httpStatusCode == errors.HttpStatusCode_INVALID_AUTH {
		errorCode = errors.ErrorCode_INVALID_AUTH
	} else if httpStatusCode == errors.HttpStatusCode_CLOCK_TOO_SKEWED {
		errorCode = errors.ErrorCode_CLOCK_TOO_SKEWED
	} else if httpStatusCode == errors.HttpStatusCode_REQUEST_TOO_LARGE {
		errorCode = errors.ErrorCode_REQUEST_TOO_LARGE
	} else if httpStatusCode == errors.HttpStatusCode_INTERNAL_ERROR {
		errorCode = errors.ErrorCode_INTERNAL_ERROR
	} else if httpStatusCode == errors.HttpStatusCode_BAD_REQUEST {
		errorCode = errors.ErrorCode_BAD_REQUEST
	}
	return &TalosTransportError{
		HttpStatusCode: httpStatusCode,
		ErrorCode:      errorCode,
		ErrorMessage:   errorMessage,
		ServerTime:     timestamp,
	}
}

func (e *TalosTransportError) GetErrorCode() errors.ErrorCode {
	return e.ErrorCode
}

func (e *TalosTransportError) String() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Transport error, ErrorCode: %s, HttpStatusCode: %s, ErrorMessage: %s",
		e.ErrorCode, e.HttpStatusCode, e.ErrorMessage)
}

func (e *TalosTransportError) Error() string {
	return e.String()
}

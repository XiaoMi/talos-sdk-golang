/**
 * Copyright 2018, Xiaomi.
 * All rights reserved.
 * Author: wangfan8@xiaomi.com
 */

package client

import (
	"fmt"

	"github.com/XiaoMi/talos-sdk-golang/thrift/common"
)

type TalosErrorCodePeeker interface {
	GetErrorCode() common.ErrorCode
}

type TalosTransportError struct {
	HttpStatusCode common.HttpStatusCode
	ErrorCode      common.ErrorCode
	ErrorMessage   string
	ServerTime     int64
}

func NewTalosTransportError(httpStatusCode common.HttpStatusCode,
	errorMessage string, timestamp int64) *TalosTransportError {
	errorCode := common.ErrorCode_UNKNOWN
	if httpStatusCode == common.HttpStatusCode_INVALID_AUTH {
		errorCode = common.ErrorCode_INVALID_AUTH_INFO
	} else if httpStatusCode == common.HttpStatusCode_CLOCK_TOO_SKEWED {
		errorCode = common.ErrorCode_CLOCK_TOO_SKEWED
	} else if httpStatusCode == common.HttpStatusCode_REQUEST_TOO_LARGE {
		errorCode = common.ErrorCode_REQUEST_TOO_LARGE
	} else if httpStatusCode == common.HttpStatusCode_INTERNAL_ERROR {
		errorCode = common.ErrorCode_INTERNAL_SERVER_ERROR
	} else if httpStatusCode == common.HttpStatusCode_BAD_REQUEST {
		errorCode = common.ErrorCode_BAD_REQUEST
	}
	return &TalosTransportError{
		HttpStatusCode: httpStatusCode,
		ErrorCode:      errorCode,
		ErrorMessage:   errorMessage,
		ServerTime:     timestamp,
	}
}

func (e *TalosTransportError) GetErrorCode() common.ErrorCode {
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

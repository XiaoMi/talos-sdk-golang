// Code generated by MockGen. DO NOT EDIT.
// Source: ScheduleInfoCache.go

// Package mock_client is a generated GoMock package.
package mock_client

import (
	reflect "reflect"

	message "talos-sdk-golang/thrift/message"
	topic "talos-sdk-golang/thrift/topic"

	x "talos-sdk-golang/client"

	gomock "github.com/golang/mock/gomock"
)

// MockScheduleInfo is a mock of ScheduleInfo interface
type MockScheduleInfo struct {
	ctrl     *gomock.Controller
	recorder *MockScheduleInfoMockRecorder
}

// MockScheduleInfoMockRecorder is the mock recorder for MockScheduleInfo
type MockScheduleInfoMockRecorder struct {
	mock *MockScheduleInfo
}

// NewMockScheduleInfo creates a new mock instance
func NewMockScheduleInfo(ctrl *gomock.Controller) *MockScheduleInfo {
	mock := &MockScheduleInfo{ctrl: ctrl}
	mock.recorder = &MockScheduleInfoMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockScheduleInfo) EXPECT() *MockScheduleInfoMockRecorder {
	return m.recorder
}

// GetOrCreateMessageClient mocks base method
func (m *MockScheduleInfo) GetOrCreateMessageClient(topicAndPartition *topic.TopicAndPartition) message.MessageService {
	ret := m.ctrl.Call(m, "GetOrCreateMessageClient", topicAndPartition)
	ret0, _ := ret[0].(message.MessageService)
	return ret0
}

// GetOrCreateMessageClient indicates an expected call of GetOrCreateMessageClient
func (mr *MockScheduleInfoMockRecorder) GetOrCreateMessageClient(topicAndPartition interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrCreateMessageClient", reflect.TypeOf((*MockScheduleInfo)(nil).GetOrCreateMessageClient), topicAndPartition)
}

// GetScheduleInfoCache mocks base method
func (m *MockScheduleInfo) GetScheduleInfoCache(topicTalosResourceName *topic.TopicTalosResourceName) *x.ScheduleInfoCache {
	ret := m.ctrl.Call(m, "GetScheduleInfoCache", topicTalosResourceName)
	ret0, _ := ret[0].(*x.ScheduleInfoCache)
	return ret0
}

// GetScheduleInfoCache indicates an expected call of GetScheduleInfoCache
func (mr *MockScheduleInfoMockRecorder) GetScheduleInfoCache(topicTalosResourceName interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetScheduleInfoCache", reflect.TypeOf((*MockScheduleInfo)(nil).GetScheduleInfoCache), topicTalosResourceName)
}

// SetIsAutoLocaton mocks base method
func (m *MockScheduleInfo) SetIsAutoLocaton(autoLocation bool) {
	m.ctrl.Call(m, "SetIsAutoLocaton", autoLocation)
}

// SetIsAutoLocaton indicates an expected call of SetIsAutoLocaton
func (mr *MockScheduleInfoMockRecorder) SetIsAutoLocaton(autoLocation interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetIsAutoLocaton", reflect.TypeOf((*MockScheduleInfo)(nil).SetIsAutoLocaton), autoLocation)
}

// IsAutoLocation mocks base method
func (m *MockScheduleInfo) IsAutoLocation() bool {
	ret := m.ctrl.Call(m, "IsAutoLocation")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsAutoLocation indicates an expected call of IsAutoLocation
func (mr *MockScheduleInfoMockRecorder) IsAutoLocation() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsAutoLocation", reflect.TypeOf((*MockScheduleInfo)(nil).IsAutoLocation))
}

// GetScheduleInfo mocks base method
func (m *MockScheduleInfo) GetScheduleInfo(name *topic.TopicTalosResourceName) error {
	ret := m.ctrl.Call(m, "GetScheduleInfo", name)
	ret0, _ := ret[0].(error)
	return ret0
}

// GetScheduleInfo indicates an expected call of GetScheduleInfo
func (mr *MockScheduleInfoMockRecorder) GetScheduleInfo(name interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetScheduleInfo", reflect.TypeOf((*MockScheduleInfo)(nil).GetScheduleInfo), name)
}

// InitGetScheduleInfoTask mocks base method
func (m *MockScheduleInfo) InitGetScheduleInfoTask() {
	m.ctrl.Call(m, "InitGetScheduleInfoTask")
}

// InitGetScheduleInfoTask indicates an expected call of InitGetScheduleInfoTask
func (mr *MockScheduleInfoMockRecorder) InitGetScheduleInfoTask() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InitGetScheduleInfoTask", reflect.TypeOf((*MockScheduleInfo)(nil).InitGetScheduleInfoTask))
}

// UpdateScheduleInfoCache mocks base method
func (m *MockScheduleInfo) UpdateScheduleInfoCache() {
	m.ctrl.Call(m, "UpdateScheduleInfoCache")
}

// UpdateScheduleInfoCache indicates an expected call of UpdateScheduleInfoCache
func (mr *MockScheduleInfoMockRecorder) UpdateScheduleInfoCache() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateScheduleInfoCache", reflect.TypeOf((*MockScheduleInfo)(nil).UpdateScheduleInfoCache))
}

// GetScheduleInfoTask mocks base method
func (m *MockScheduleInfo) GetScheduleInfoTask() error {
	ret := m.ctrl.Call(m, "GetScheduleInfoTask")
	ret0, _ := ret[0].(error)
	return ret0
}

// GetScheduleInfoTask indicates an expected call of GetScheduleInfoTask
func (mr *MockScheduleInfoMockRecorder) GetScheduleInfoTask() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetScheduleInfoTask", reflect.TypeOf((*MockScheduleInfo)(nil).GetScheduleInfoTask))
}

// Shutdown mocks base method
func (m *MockScheduleInfo) Shutdown(topicTalosResourceName *topic.TopicTalosResourceName) {
	m.ctrl.Call(m, "Shutdown", topicTalosResourceName)
}

// Shutdown indicates an expected call of Shutdown
func (mr *MockScheduleInfoMockRecorder) Shutdown(topicTalosResourceName interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Shutdown", reflect.TypeOf((*MockScheduleInfo)(nil).Shutdown), topicTalosResourceName)
}

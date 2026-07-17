package client

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
)

type talosHTTPDrainBody struct {
	data      []byte
	readCalls int
	closed    bool
}

func (b *talosHTTPDrainBody) Read(p []byte) (int, error) {
	b.readCalls++
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func (b *talosHTTPDrainBody) Close() error {
	b.closed = true
	return nil
}

func TestTalosHttpClientCloseDrainsPendingEOF(t *testing.T) {
	body := &talosHTTPDrainBody{}
	client := &TalosHttpClient{response: &http.Response{Body: body}}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if body.readCalls != 1 {
		t.Fatalf("readCalls = %d, want 1", body.readCalls)
	}
	if !body.closed {
		t.Fatal("body was not closed")
	}
}

func TestTalosHttpClientCloseStopsDrainAtLimit(t *testing.T) {
	body := &talosHTTPDrainBody{data: make([]byte, maxResponseBodySlurpSize+10)}
	client := &TalosHttpClient{response: &http.Response{Body: body}}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if len(body.data) != 10 {
		t.Fatalf("remaining body bytes = %d, want 10", len(body.data))
	}
	if !body.closed {
		t.Fatal("body was not closed")
	}
}

func TestTalosHttpClientCloseDrainAllowsChunkedConnectionReuse(t *testing.T) {
	var newConnCount int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))
		w.(http.Flusher).Flush()
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			atomic.AddInt32(&newConnCount, 1)
		}
	}
	server.Start()
	defer server.Close()

	transport := &http.Transport{MaxIdleConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport}
	secretKeyId := "secretKeyId"
	secretKey := "secretKey"
	credential := &auth.Credential{SecretKeyId: &secretKeyId, SecretKey: &secretKey}

	doRequest := func() {
		trans, err := NewTalosHttpClient(server.URL, credential, httpClient, "test-agent", 0, "type=test")
		if err != nil {
			t.Fatalf("NewTalosHttpClient() error = %v", err)
		}
		client := trans.(*TalosHttpClient)
		if _, err := client.Write([]byte("request")); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := client.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
		buf := make([]byte, 1)
		n, err := client.Read(buf)
		if err != nil || n != 1 || buf[0] != 'x' {
			t.Fatalf("Read() = n:%d buf:%q err:%v, want one x byte", n, string(buf[:n]), err)
		}
		if err := client.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}

	doRequest()
	doRequest()

	if got := atomic.LoadInt32(&newConnCount); got != 1 {
		t.Fatalf("new TCP connections = %d, want 1", got)
	}
}

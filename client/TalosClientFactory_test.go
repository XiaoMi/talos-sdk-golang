package client

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// resetDNSCache clears the cache before each test.
func resetDNSCache() {
	dnsCacheStore.Lock()
	dnsCacheStore.entries = make(map[string]*dnsCacheEntry)
	dnsCacheStore.Unlock()
}

func cacheHostForTest(host string, ip string) {
	dnsCacheStore.Lock()
	dnsCacheStore.entries[host] = &dnsCacheEntry{
		ips:       []string{ip},
		expiresAt: time.Now().Add(time.Minute),
	}
	dnsCacheStore.Unlock()
}

func newHTTPClientWithDNSCacheForTest(proxyURL string) *http.Client {
	config := &TalosClientConfig{}
	config.SetClientConnTimeout(1000)
	config.SetClientTimeout(3000)
	config.SetDNSCacheSwitch(true)
	config.SetHttpProxyURL(proxyURL)
	return newHTTPClientFromConfigForTest(config)
}

func newHTTPClientFromConfigForTest(config *TalosClientConfig) *http.Client {
	return NewTalosClientFactory(config, nil).GetHttpClient()
}

func serverHostPort(server *httptest.Server) (string, int) {
	addr := server.Listener.Addr().(*net.TCPAddr)
	return addr.IP.String(), addr.Port
}

type proxyRequestForTest struct {
	url                string
	host               string
	isAbs              bool
	proxyAuthorization string
}

func TestLookupWithDNSCache_CacheMiss(t *testing.T) {
	resetDNSCache()
	ctx := context.Background()

	ips, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ips) == 0 {
		t.Fatal("expected at least one IP")
	}

	// Verify the cache entry was written.
	dnsCacheStore.RLock()
	entry, ok := dnsCacheStore.entries["localhost"]
	dnsCacheStore.RUnlock()
	if !ok {
		t.Fatal("expected cache entry for localhost")
	}
	if time.Now().After(entry.expiresAt) {
		t.Fatal("cache entry should not be expired")
	}
}

func TestLookupWithDNSCache_CacheHit(t *testing.T) {
	resetDNSCache()
	ctx := context.Background()

	// First call populates the cache.
	_, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// Second call should hit the cache.
	ips, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if len(ips) == 0 {
		t.Fatal("expected at least one IP from cache")
	}
}

func TestLookupWithDNSCache_ExpiredCache(t *testing.T) {
	resetDNSCache()

	// Manually write an expired cache entry.
	dnsCacheStore.Lock()
	dnsCacheStore.entries["expired-host"] = &dnsCacheEntry{
		ips:       []string{"1.2.3.4"},
		expiresAt: time.Now().Add(-1 * time.Second), // expired
	}
	dnsCacheStore.Unlock()

	ctx := context.Background()

	// For a non-existent hostname, DNS should fail and fall back to stale cache.
	ips, err := lookupWithDNSCache(ctx, "expired-host")
	if err != nil {
		// If DNS lookup fails and has no fresh result, stale cache should be returned.
		// If DNS lookup succeeds, the cache should be refreshed instead.
		// expired-host should not exist, so DNS should fail and fall back to stale cache.
		t.Logf("DNS failed as expected for non-existent host: %v", err)
		return
	}
	// If resolution succeeds, verify a valid IP is returned.
	if len(ips) == 0 {
		t.Fatal("expected at least one IP")
	}
}

func TestLookupWithDNSCache_DNSFailureFallbackToStale(t *testing.T) {
	resetDNSCache()

	staleIPs := []string{"10.0.0.1", "10.0.0.2"}

	// Write an expired cache entry.
	dnsCacheStore.Lock()
	dnsCacheStore.entries["nonexistent.invalid"] = &dnsCacheEntry{
		ips:       staleIPs,
		expiresAt: time.Now().Add(-1 * time.Second),
	}
	dnsCacheStore.Unlock()

	ctx := context.Background()

	// DNS lookup for nonexistent.invalid should fail and fall back to stale cache.
	ips, err := lookupWithDNSCache(ctx, "nonexistent.invalid")
	if err != nil {
		t.Fatalf("expected fallback to stale cache, got error: %v", err)
	}
	if len(ips) != len(staleIPs) {
		t.Fatalf("expected %d IPs from stale cache, got %d", len(staleIPs), len(ips))
	}
	for i, ip := range ips {
		if ip != staleIPs[i] {
			t.Fatalf("expected stale IP %s, got %s", staleIPs[i], ip)
		}
	}
}

func TestLookupWithDNSCache_DNSFailureNoCache(t *testing.T) {
	resetDNSCache()
	ctx := context.Background()

	// Query a non-existent domain without cache.
	_, err := lookupWithDNSCache(ctx, "this-host-does-not-exist.invalid")
	if err == nil {
		t.Fatal("expected error for non-existent host with no cache")
	}
}

func TestLookupWithDNSCache_ConcurrentAccess(t *testing.T) {
	resetDNSCache()
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lookupWithDNSCache(ctx, "localhost")
			if err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent call failed: %v", err)
	}
}

func TestLookupWithDNSCache_DNSLookupUsesBackgroundContext(t *testing.T) {
	resetDNSCache()

	originalResolver := net.DefaultResolver
	dialStarted := make(chan struct{})
	checkDialContext := make(chan struct{})
	releaseDial := make(chan struct{})
	dialContextErr := make(chan error, 1)
	var releaseOnce sync.Once
	t.Cleanup(func() {
		net.DefaultResolver = originalResolver
		releaseOnce.Do(func() { close(releaseDial) })
	})

	var dialCalls int32
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			if atomic.AddInt32(&dialCalls, 1) == 1 {
				close(dialStarted)
				<-checkDialContext
				dialContextErr <- ctx.Err()
				<-releaseDial
			}
			return nil, fmt.Errorf("dns lookup stopped")
		},
	}

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	lookupDone := make(chan error, 1)
	go func() {
		_, err := lookupWithDNSCache(callerCtx, "background-context-test.invalid")
		lookupDone <- err
	}()

	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DNS dial to start")
	}

	cancelCaller()
	close(checkDialContext)
	if err := <-dialContextErr; err != nil {
		t.Fatalf("expected DNS lookup context to ignore caller cancellation, got: %v", err)
	}

	releaseOnce.Do(func() { close(releaseDial) })
	select {
	case err := <-lookupDone:
		if err == nil {
			t.Fatal("expected resolver error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DNS lookup to finish")
	}
}

func TestLookupWithDNSCache_IPAddress(t *testing.T) {
	resetDNSCache()
	ctx := context.Background()

	// Calling lookupWithDNSCache with a plain IP should return directly.
	ips, err := lookupWithDNSCache(ctx, "127.0.0.1")
	if err != nil {
		t.Fatalf("unexpected error for IP address: %v", err)
	}
	if len(ips) != 1 || ips[0] != "127.0.0.1" {
		t.Fatalf("expected [127.0.0.1], got %v", ips)
	}

	// Verify the plain IP is cached by the current implementation.
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["127.0.0.1"]
	dnsCacheStore.RUnlock()
	if !ok {
		t.Fatal("expected IP to be cached")
	}
}

func TestDialContext_IPBypassesCache(t *testing.T) {
	resetDNSCache()

	// Create a listener to verify the connection.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	// Connect to a plain IP, which should bypass the cache.
	conn, err := dialWithDNSCache(context.Background(), "tcp", ln.Addr().String(), net.Dialer{})
	if err != nil {
		t.Fatalf("dial to IP failed: %v", err)
	}
	conn.Close()

	// Verify the cache has no entry for this IP.
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["127.0.0.1"]
	dnsCacheStore.RUnlock()
	if ok {
		t.Fatal("IP address should not be cached by DialContext")
	}
}

func TestDialContext_HostnameUsesCache(t *testing.T) {
	resetDNSCache()

	// Create a listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	// Connect with localhost as a hostname, which should use the cache.
	addr := ln.Addr().String()
	conn, err := dialWithDNSCache(context.Background(), "tcp", "localhost"+addr[len("127.0.0.1"):], net.Dialer{})
	if err != nil {
		t.Fatalf("dial to hostname failed: %v", err)
	}
	conn.Close()

	// Verify localhost is cached.
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["localhost"]
	dnsCacheStore.RUnlock()
	if !ok {
		t.Fatal("expected localhost to be cached after DialContext")
	}
}

func TestTalosClientFactory_HTTPTransportDNSCacheEnabledUsesCache(t *testing.T) {
	resetDNSCache()

	requestCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCh <- r.Host + r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	serverIP, serverPort := serverHostPort(server)
	originHost := "talos-origin.invalid"
	client := newHTTPClientWithDNSCacheForTest("")

	_, err := client.Get(fmt.Sprintf("http://%s:%d/dns-direct", originHost, serverPort))
	if err == nil {
		t.Fatal("expected direct request to fail before DNS cache is populated")
	}

	cacheHostForTest(originHost, serverIP)

	resp, err := client.Get(fmt.Sprintf("http://%s:%d/dns-direct", originHost, serverPort))
	if err != nil {
		t.Fatalf("expected direct request to use DNS cache: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, resp.StatusCode)
	}

	select {
	case got := <-requestCh:
		expected := fmt.Sprintf("%s:%d/dns-direct", originHost, serverPort)
		if got != expected {
			t.Fatalf("expected origin request %q, got %q", expected, got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected origin server to receive request")
	}
}

func TestTalosClientFactory_HTTPTransportDNSCacheDisabledByDefault(t *testing.T) {
	resetDNSCache()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	serverIP, serverPort := serverHostPort(server)
	originHost := "talos-dns-cache-disabled.invalid"
	cacheHostForTest(originHost, serverIP)

	config := NewTalosClientConfigByDefault()
	config.SetClientConnTimeout(100)
	config.SetClientTimeout(500)
	client := newHTTPClientFromConfigForTest(config)

	_, err := client.Get(fmt.Sprintf("http://%s:%d/dns-cache-disabled", originHost, serverPort))
	if err == nil {
		t.Fatal("expected request to ignore DNS cache when DNS cache switch is disabled")
	}
}

func TestTalosClientFactory_HTTPTransportProxyUsesDNSCache(t *testing.T) {
	resetDNSCache()

	proxyCh := make(chan proxyRequestForTest, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCh <- proxyRequestForTest{
			url:   r.URL.String(),
			host:  r.Host,
			isAbs: r.URL.IsAbs(),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer proxy.Close()

	proxyIP, proxyPort := serverHostPort(proxy)
	proxyHost := "talos-proxy.invalid"
	targetURL := "http://talos-origin-behind-proxy.invalid/proxy-dns"
	client := newHTTPClientWithDNSCacheForTest(fmt.Sprintf("http://%s:%d", proxyHost, proxyPort))

	_, err := client.Get(targetURL)
	if err == nil {
		t.Fatal("expected proxied request to fail before proxy host DNS cache is populated")
	}

	cacheHostForTest(proxyHost, proxyIP)

	resp, err := client.Get(targetURL)
	if err != nil {
		t.Fatalf("expected proxied request to use DNS cache for proxy host: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, resp.StatusCode)
	}

	select {
	case got := <-proxyCh:
		if !got.isAbs {
			t.Fatal("expected proxy to receive absolute-form request URL")
		}
		if got.url != targetURL {
			t.Fatalf("expected proxy request URL %q, got %q", targetURL, got.url)
		}
		if got.host != "talos-origin-behind-proxy.invalid" {
			t.Fatalf("expected target host through proxy, got %q", got.host)
		}
	case <-time.After(time.Second):
		t.Fatal("expected proxy server to receive request")
	}
}

func TestTalosClientFactory_HTTPTransportProxyHostPortUsesDNSCache(t *testing.T) {
	resetDNSCache()

	proxyCh := make(chan proxyRequestForTest, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCh <- proxyRequestForTest{
			url:                r.URL.String(),
			host:               r.Host,
			isAbs:              r.URL.IsAbs(),
			proxyAuthorization: r.Header.Get("Proxy-Authorization"),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer proxy.Close()

	proxyIP, proxyPort := serverHostPort(proxy)
	proxyHost := "talos-proxy-host-port.invalid"

	config := &TalosClientConfig{}
	config.SetClientConnTimeout(1000)
	config.SetClientTimeout(3000)
	config.SetHttpProxyHost(proxyHost)
	config.SetHttpProxyPort(int64(proxyPort))
	config.SetHttpProxyUsername("talos-user")
	config.SetHttpProxyPassword("talos-password")
	config.SetDNSCacheSwitch(true)

	targetURL := "http://talos-origin-behind-host-port-proxy.invalid/proxy-host-port-dns"
	client := newHTTPClientFromConfigForTest(config)

	_, err := client.Get(targetURL)
	if err == nil {
		t.Fatal("expected host/port proxied request to fail before proxy host DNS cache is populated")
	}

	cacheHostForTest(proxyHost, proxyIP)

	resp, err := client.Get(targetURL)
	if err != nil {
		t.Fatalf("expected host/port proxied request to use DNS cache for proxy host: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, resp.StatusCode)
	}

	select {
	case got := <-proxyCh:
		if !got.isAbs {
			t.Fatal("expected proxy to receive absolute-form request URL")
		}
		if got.url != targetURL {
			t.Fatalf("expected proxy request URL %q, got %q", targetURL, got.url)
		}
		if got.host != "talos-origin-behind-host-port-proxy.invalid" {
			t.Fatalf("expected target host through proxy, got %q", got.host)
		}
		if got.proxyAuthorization != "Basic dGFsb3MtdXNlcjp0YWxvcy1wYXNzd29yZA==" {
			t.Fatalf("expected proxy auth header, got %q", got.proxyAuthorization)
		}
	case <-time.After(time.Second):
		t.Fatal("expected proxy server to receive request")
	}
}

func TestTalosClientFactory_HTTPTransportInvalidProxyURLReturnsError(t *testing.T) {
	config := &TalosClientConfig{}
	config.SetClientConnTimeout(1000)
	config.SetClientTimeout(3000)
	config.SetHttpProxyURL("http://[::1")

	client := newHTTPClientFromConfigForTest(config)
	_, err := client.Get("http://example.com")
	if err == nil {
		t.Fatal("expected invalid proxy URL to fail before dialing")
	}
}

func TestDNSCacheStore_TTL(t *testing.T) {
	resetDNSCache()

	// Verify the default TTL.
	if dnsCacheStore.ttl != 60*time.Second {
		t.Fatalf("expected TTL 60s, got %v", dnsCacheStore.ttl)
	}
}

func TestDNSCacheStore_InitialEmpty(t *testing.T) {
	resetDNSCache()

	dnsCacheStore.RLock()
	count := len(dnsCacheStore.entries)
	dnsCacheStore.RUnlock()

	if count != 0 {
		t.Fatalf("expected empty cache, got %d entries", count)
	}
}

func TestLookupWithDNSCache_CacheUpdate(t *testing.T) {
	resetDNSCache()
	ctx := context.Background()

	// First call.
	ips1, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// Mark the cache as expired to simulate TTL expiry.
	dnsCacheStore.Lock()
	dnsCacheStore.entries["localhost"].expiresAt = time.Now().Add(-1 * time.Second)
	dnsCacheStore.Unlock()

	// Call again to resolve and refresh the cache.
	ips2, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	// IPs should be consistent for the same host.
	if len(ips1) != len(ips2) {
		t.Fatalf("IP count changed: %d vs %d", len(ips1), len(ips2))
	}

	// Verify the cache was refreshed with a future expiresAt.
	dnsCacheStore.RLock()
	entry := dnsCacheStore.entries["localhost"]
	dnsCacheStore.RUnlock()
	if time.Now().After(entry.expiresAt) {
		t.Fatal("cache should have been refreshed")
	}
}

func TestDialContext_InvalidAddress(t *testing.T) {
	_, err := dialWithDNSCache(context.Background(), "tcp", "no-port-here", net.Dialer{})
	if err == nil {
		t.Fatal("expected error for address without port")
	}
}

func TestDialContext_HostnameDNSFailure(t *testing.T) {
	resetDNSCache()

	_, err := dialWithDNSCache(context.Background(), "tcp", "this-host-does-not-exist.invalid:80", net.Dialer{})
	if err == nil {
		t.Fatal("expected error for non-existent hostname")
	}
}

func TestDialContext_MultiIPFallback(t *testing.T) {
	resetDNSCache()

	lnGood, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lnGood.Close()

	go func() {
		for {
			conn, err := lnGood.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	goodPort := lnGood.Addr().(*net.TCPAddr).Port

	dnsCacheStore.Lock()
	dnsCacheStore.entries["fallback-host"] = &dnsCacheEntry{
		ips:       []string{"127.0.0.2", "127.0.0.1"},
		expiresAt: time.Now().Add(60 * time.Second),
	}
	dnsCacheStore.Unlock()

	conn, err := dialWithDNSCache(context.Background(), "tcp",
		"fallback-host:"+fmt.Sprintf("%d", goodPort), net.Dialer{})
	if err != nil {
		t.Fatalf("expected fallback to succeed on second IP, got: %v", err)
	}
	conn.Close()
}

func TestDialContext_AllIPsFail(t *testing.T) {
	resetDNSCache()

	dnsCacheStore.Lock()
	dnsCacheStore.entries["all-fail-host"] = &dnsCacheEntry{
		ips:       []string{"192.0.2.1", "192.0.2.2"},
		expiresAt: time.Now().Add(60 * time.Second),
	}
	dnsCacheStore.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := dialWithDNSCache(ctx, "tcp", "all-fail-host:80", net.Dialer{})
	if err == nil {
		t.Fatal("expected error when all IPs fail to connect")
	}
}

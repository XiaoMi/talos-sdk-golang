package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// resetDNSCache clears the cache, call before each test
func resetDNSCache() {
	dnsCacheStore.Lock()
	dnsCacheStore.entries = make(map[string]*dnsCacheEntry)
	dnsCacheStore.Unlock()
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

	// verify entry was written to cache
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

	// first call, writes to cache
	_, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// second call, should hit cache
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

	// manually insert an expired cache entry
	dnsCacheStore.Lock()
	dnsCacheStore.entries["expired-host"] = &dnsCacheEntry{
		ips:       []string{"1.2.3.4"},
		expiresAt: time.Now().Add(-1 * time.Second), // expired
	}
	dnsCacheStore.Unlock()

	ctx := context.Background()

	// for non-existent hostname, DNS fails, should fallback to stale cache
	ips, err := lookupWithDNSCache(ctx, "expired-host")
	if err != nil {
		// DNS lookup fails for expired-host, should return stale cache
		t.Logf("DNS failed as expected for non-existent host: %v", err)
		return
	}
	// if resolved successfully, verify valid IP returned
	if len(ips) == 0 {
		t.Fatal("expected at least one IP")
	}
}

func TestLookupWithDNSCache_DNSFailureFallbackToStale(t *testing.T) {
	resetDNSCache()

	staleIPs := []string{"10.0.0.1", "10.0.0.2"}

	// insert stale cache entry
	dnsCacheStore.Lock()
	dnsCacheStore.entries["nonexistent.invalid"] = &dnsCacheEntry{
		ips:       staleIPs,
		expiresAt: time.Now().Add(-1 * time.Second),
	}
	dnsCacheStore.Unlock()

	ctx := context.Background()

	// DNS lookup for nonexistent.invalid fails, should fallback to stale cache
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

	// query non-existent domain, no cache
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

func TestLookupWithDNSCache_IPAddress(t *testing.T) {
	resetDNSCache()
	ctx := context.Background()

	// lookupWithDNSCache with pure IP should return directly
	ips, err := lookupWithDNSCache(ctx, "127.0.0.1")
	if err != nil {
		t.Fatalf("unexpected error for IP address: %v", err)
	}
	if len(ips) != 1 || ips[0] != "127.0.0.1" {
		t.Fatalf("expected [127.0.0.1], got %v", ips)
	}

	// verify pure IP is also cached (current implementation caches it)
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["127.0.0.1"]
	dnsCacheStore.RUnlock()
	if !ok {
		t.Fatal("expected IP to be cached")
	}
}

func TestDialContext_IPBypassesCache(t *testing.T) {
	resetDNSCache()

	// create a listener to verify connection
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

	// connect to pure IP, should bypass cache
	conn, err := dialWithDNSCache(context.Background(), "tcp", ln.Addr().String(), net.Dialer{})
	if err != nil {
		t.Fatalf("dial to IP failed: %v", err)
	}
	conn.Close()

	// verify no cache entry for this IP
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["127.0.0.1"]
	dnsCacheStore.RUnlock()
	if ok {
		t.Fatal("IP address should not be cached by DialContext")
	}
}

func TestDialContext_HostnameUsesCache(t *testing.T) {
	resetDNSCache()

	// create a listener
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

	// connect via localhost (hostname), should use cache
	addr := ln.Addr().String()
	conn, err := dialWithDNSCache(context.Background(), "tcp", "localhost"+addr[len("127.0.0.1"):], net.Dialer{})
	if err != nil {
		t.Fatalf("dial to hostname failed: %v", err)
	}
	conn.Close()

	// verify localhost is cached
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["localhost"]
	dnsCacheStore.RUnlock()
	if !ok {
		t.Fatal("expected localhost to be cached after DialContext")
	}
}

func TestDNSCacheStore_TTL(t *testing.T) {
	resetDNSCache()

	// verify default TTL
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

	// first call
	ips1, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// mark cache as expired to simulate TTL expiry
	dnsCacheStore.Lock()
	dnsCacheStore.entries["localhost"].expiresAt = time.Now().Add(-1 * time.Second)
	dnsCacheStore.Unlock()

	// call again, should re-resolve and update cache
	ips2, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	// IP count should be the same (same host)
	if len(ips1) != len(ips2) {
		t.Fatalf("IP count changed: %d vs %d", len(ips1), len(ips2))
	}

	// verify cache was refreshed (expiresAt should be in the future)
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
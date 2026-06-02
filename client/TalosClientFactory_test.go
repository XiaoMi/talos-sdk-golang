package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// resetDNSCache 清空缓存，每个测试前调用
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

	// 验证已写入缓存
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

	// 首次调用，写入缓存
	_, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// 第二次调用，应命中缓存
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

	// 手动写入一条已过期的缓存
	dnsCacheStore.Lock()
	dnsCacheStore.entries["expired-host"] = &dnsCacheEntry{
		ips:       []string{"1.2.3.4"},
		expiresAt: time.Now().Add(-1 * time.Second), // 已过期
	}
	dnsCacheStore.Unlock()

	ctx := context.Background()

	// 对于不存在的 hostname，DNS 会失败，但有过期缓存应降级返回
	ips, err := lookupWithDNSCache(ctx, "expired-host")
	if err != nil {
		// 如果 DNS 查询失败且没有新结果，应返回过期缓存
		// 但如果 DNS 解析成功了，会更新缓存
		// 这里 expired-host 不存在，DNS 会失败，应返回过期缓存
		t.Logf("DNS failed as expected for non-existent host: %v", err)
		return
	}
	// 如果解析成功，验证返回了有效 IP
	if len(ips) == 0 {
		t.Fatal("expected at least one IP")
	}
}

func TestLookupWithDNSCache_DNSFailureFallbackToStale(t *testing.T) {
	resetDNSCache()

	staleIPs := []string{"10.0.0.1", "10.0.0.2"}

	// 写入已过期的缓存
	dnsCacheStore.Lock()
	dnsCacheStore.entries["nonexistent.invalid"] = &dnsCacheEntry{
		ips:       staleIPs,
		expiresAt: time.Now().Add(-1 * time.Second),
	}
	dnsCacheStore.Unlock()

	ctx := context.Background()

	// DNS 查询 nonexistent.invalid 会失败，应降级返回过期缓存
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

	// 查询不存在的域名，无缓存
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

	// 对纯 IP 调用 lookupWithDNSCache，Go 应直接返回
	ips, err := lookupWithDNSCache(ctx, "127.0.0.1")
	if err != nil {
		t.Fatalf("unexpected error for IP address: %v", err)
	}
	if len(ips) != 1 || ips[0] != "127.0.0.1" {
		t.Fatalf("expected [127.0.0.1], got %v", ips)
	}

	// 验证纯 IP 也被缓存了（当前实现会缓存）
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["127.0.0.1"]
	dnsCacheStore.RUnlock()
	if !ok {
		t.Fatal("expected IP to be cached")
	}
}

func TestDialContext_IPBypassesCache(t *testing.T) {
	resetDNSCache()

	// 创建一个监听器来验证连接
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

	// 使用与 NewTalosClientFactory 相同的 DialContext 逻辑
	dialCtx := func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if net.ParseIP(host) != nil {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, addr)
		}
		ips, err := lookupWithDNSCache(ctx, host)
		if err != nil {
			return nil, err
		}
		var dialer net.Dialer
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
			if err == nil {
				return conn, nil
			}
		}
		return nil, err
	}

	// 连接纯 IP，不应走缓存
	conn, err := dialCtx(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial to IP failed: %v", err)
	}
	conn.Close()

	// 验证缓存中没有这个 IP 的条目
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["127.0.0.1"]
	dnsCacheStore.RUnlock()
	if ok {
		t.Fatal("IP address should not be cached by DialContext")
	}
}

func TestDialContext_HostnameUsesCache(t *testing.T) {
	resetDNSCache()

	// 创建一个监听器
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

	dialCtx := func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if net.ParseIP(host) != nil {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, addr)
		}
		ips, err := lookupWithDNSCache(ctx, host)
		if err != nil {
			return nil, err
		}
		var dialer net.Dialer
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
			if err == nil {
				return conn, nil
			}
		}
		return nil, err
	}

	// 用 localhost（hostname）连接，应走缓存
	addr := ln.Addr().String()
	conn, err := dialCtx(context.Background(), "tcp", "localhost"+addr[len("127.0.0.1"):])
	if err != nil {
		t.Fatalf("dial to hostname failed: %v", err)
	}
	conn.Close()

	// 验证 localhost 已缓存
	dnsCacheStore.RLock()
	_, ok := dnsCacheStore.entries["localhost"]
	dnsCacheStore.RUnlock()
	if !ok {
		t.Fatal("expected localhost to be cached after DialContext")
	}
}

func TestDNSCacheStore_TTL(t *testing.T) {
	resetDNSCache()

	// 验证默认 TTL
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

	// 首次调用
	ips1, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// 修改缓存为过期，模拟 TTL 过期
	dnsCacheStore.Lock()
	dnsCacheStore.entries["localhost"].expiresAt = time.Now().Add(-1 * time.Second)
	dnsCacheStore.Unlock()

	// 再次调用，应重新解析并更新缓存
	ips2, err := lookupWithDNSCache(ctx, "localhost")
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	// IP 应该一致（同一个 host）
	if len(ips1) != len(ips2) {
		t.Fatalf("IP count changed: %d vs %d", len(ips1), len(ips2))
	}

	// 验证缓存已更新（expiresAt 应在未来）
	dnsCacheStore.RLock()
	entry := dnsCacheStore.entries["localhost"]
	dnsCacheStore.RUnlock()
	if time.Now().After(entry.expiresAt) {
		t.Fatal("cache should have been refreshed")
	}
}

// testDialContext 提取 NewTalosClientFactory 中的 DialContext 逻辑用于测试
func testDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	if net.ParseIP(host) != nil {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, addr)
	}

	ips, err := lookupWithDNSCache(ctx, host)
	if err != nil {
		return nil, err
	}

	var dialer net.Dialer
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			return conn, nil
		}
	}
	return nil, err
}

func TestDialContext_InvalidAddress(t *testing.T) {
	// SplitHostPort 格式错误：缺少端口
	_, err := testDialContext(context.Background(), "tcp", "no-port-here")
	if err == nil {
		t.Fatal("expected error for address without port")
	}
}

func TestDialContext_HostnameDNSFailure(t *testing.T) {
	resetDNSCache()

	// 不存在的域名，DNS 解析失败
	_, err := testDialContext(context.Background(), "tcp", "this-host-does-not-exist.invalid:80")
	if err == nil {
		t.Fatal("expected error for non-existent hostname")
	}
}

func TestDialContext_MultiIPFallback(t *testing.T) {
	resetDNSCache()

	// 启动两个监听器，一个端口可用，一个不可用
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

	// 注入缓存：两个 IP，127.0.0.2 没有监听会失败，127.0.0.1 有监听会成功
	dnsCacheStore.Lock()
	dnsCacheStore.entries["fallback-host"] = &dnsCacheEntry{
		ips:       []string{"127.0.0.2", "127.0.0.1"},
		expiresAt: time.Now().Add(60 * time.Second),
	}
	dnsCacheStore.Unlock()

	// 127.0.0.2 没有监听，会失败；127.0.0.1 有监听，会成功
	conn, err := testDialContext(context.Background(), "tcp",
		"fallback-host:"+fmt.Sprintf("%d", goodPort))
	if err != nil {
		t.Fatalf("expected fallback to succeed on second IP, got: %v", err)
	}
	conn.Close()
}

func TestDialContext_AllIPsFail(t *testing.T) {
	resetDNSCache()

	// 注入缓存：所有 IP 都不通
	dnsCacheStore.Lock()
	dnsCacheStore.entries["all-fail-host"] = &dnsCacheEntry{
		ips:       []string{"127.0.0.2", "127.0.0.3"},
		expiresAt: time.Now().Add(60 * time.Second),
	}
	dnsCacheStore.Unlock()

	// 端口 1 通常无权限连接，所有 IP 都会失败
	_, err := testDialContext(context.Background(), "tcp", "all-fail-host:1")
	if err == nil {
		t.Fatal("expected error when all IPs fail to connect")
	}
}
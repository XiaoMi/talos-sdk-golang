package client

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type perHostTransport struct {
	newTransport func() *http.Transport
	hosts        sync.Map
}

var _ http.RoundTripper = (*perHostTransport)(nil)

func newTalosHTTPRoundTripper(config *TalosClientConfig) http.RoundTripper {
	transportFactory := func() *http.Transport {
		return newTalosHTTPTransport(config)
	}
	transport := transportFactory()
	if config.HttpProxyPerHostTransport() && transport.Proxy != nil {
		return newPerHostTransport(transportFactory)
	}
	return transport
}

func newPerHostTransport(transportFactory func() *http.Transport) *perHostTransport {
	return &perHostTransport{newTransport: transportFactory}
}

func newTalosHTTPTransport(config *TalosClientConfig) *http.Transport {
	transport := &http.Transport{
		MaxIdleConns:          int(config.MaxIdleConns()),
		MaxIdleConnsPerHost:   int(config.MaxIdleConnsPerHost()),
		MaxConnsPerHost:       int(config.MaxConnsPerHost()),
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext: newDNSCacheDialContext(config.ClientConnTimeout(),
			config.DNSCacheSwitch()),
	}
	configureTalosHTTPProxy(transport, config)
	return transport
}

func configureTalosHTTPProxy(transport *http.Transport, config *TalosClientConfig) {
	if proxyURL := config.HttpProxyURL(); proxyURL != "" {
		parsedProxyURL, err := url.Parse(proxyURL)
		if err != nil {
			transport.Proxy = func(*http.Request) (*url.URL, error) {
				return nil, err
			}
			return
		}
		transport.Proxy = http.ProxyURL(parsedProxyURL)
		return
	}

	if config.HttpProxyHost() == "" || config.HttpProxyPort() <= 0 {
		return
	}
	proxyURL := &url.URL{
		Scheme: "http",
		Host: net.JoinHostPort(config.HttpProxyHost(),
			strconv.FormatInt(config.HttpProxyPort(), 10)),
	}
	if config.HttpProxyUsername() != "" && config.HttpProxyPassword() != "" {
		proxyURL.User = url.UserPassword(config.HttpProxyUsername(),
			config.HttpProxyPassword())
	}
	transport.Proxy = http.ProxyURL(proxyURL)
}

func (t *perHostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Host
	if host == "" {
		host = req.Host
	}
	return t.transportForHost(host).RoundTrip(req)
}

func (t *perHostTransport) transportForHost(host string) *http.Transport {
	if tr, ok := t.hosts.Load(host); ok {
		return tr.(*http.Transport)
	}
	tr := t.newTransport()
	actual, loaded := t.hosts.LoadOrStore(host, tr)
	if loaded {
		tr.CloseIdleConnections()
	}
	return actual.(*http.Transport)
}

func (t *perHostTransport) CloseIdleConnections() {
	t.hosts.Range(func(_, value interface{}) bool {
		value.(*http.Transport).CloseIdleConnections()
		return true
	})
}

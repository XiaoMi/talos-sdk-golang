/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
**/

package client

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/XiaoMi/talos-sdk-golang/thrift/auth"
	"github.com/XiaoMi/talos-sdk-golang/thrift/common"
	"github.com/XiaoMi/talos-sdk-golang/thrift/thrift"
	"github.com/gofrs/uuid"
	log "github.com/sirupsen/logrus"
)

const RequestIdLength = 8

type TalosHttpClient struct {
	url           *url.URL
	credential    *auth.Credential
	httpClient    *http.Client
	agent         string
	clockOffset   int64
	queryString   string
	requestBuffer *bytes.Buffer
	response      *http.Response
	header        http.Header
}

type TalosHttpClientTransportFactory struct {
	url        string
	credential *auth.Credential
	httpClient *http.Client
	agent      string
}

func NewTalosHttpClientTransportFactory(url string, credential *auth.Credential,
	httpClient *http.Client, agent string) *TalosHttpClientTransportFactory {
	return &TalosHttpClientTransportFactory{
		url:        url,
		credential: credential,
		httpClient: httpClient,
		agent:      agent,
	}
}

func (p *TalosHttpClientTransportFactory) GetTransport(
	transport thrift.TTransport) thrift.TTransport {
	return p.GetTransportWithClockOffset(transport, 0, "")
}

func (p *TalosHttpClientTransportFactory) GetTransportWithClockOffset(
	trans thrift.TTransport, clockOffset int64, query string) thrift.TTransport {
	if trans != nil {
		t, ok := trans.(*TalosHttpClient)
		if ok && t.url != nil {
			t2, _ := NewTalosHttpClient(t.url.String(), t.credential, t.httpClient,
				t.agent, t.clockOffset, query)
			return t2
		}
	}
	t2, _ := NewTalosHttpClient(p.url, p.credential, p.httpClient,
		p.agent, clockOffset, query)
	return t2
}

func NewTalosHttpClient(urlStr string, credential *auth.Credential,
	httpClient *http.Client, agent string, clockOffset int64,
	queryString string) (thrift.TTransport, error) {
	parsedUrl, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1024)
	return &TalosHttpClient{
		url:           parsedUrl,
		credential:    credential,
		httpClient:    httpClient,
		agent:         agent,
		clockOffset:   clockOffset,
		queryString:   queryString,
		requestBuffer: bytes.NewBuffer(buf),
		header:        http.Header{},
	}, nil
}

// Set the HTTP Header for this specific Thrift Transport
// It is important that you first assert the TTransport as a TalosHttpClient type
// like so:
//
// httpTrans := trans.(TalosHttpClient)
// httpTrans.SetHeader("User-Agent","Thrift Client 1.0")
func (p *TalosHttpClient) SetHeader(key string, value string) {
	p.header.Add(key, value)
}

// Get the HTTP Header represented by the supplied Header Key for this specific Thrift Transport
// It is important that you first assert the TTransport as a TalosHttpClient type
// like so:
//
// httpTrans := trans.(TalosHttpClient)
// hdrValue := httpTrans.GetHeader("User-Agent")
func (p *TalosHttpClient) GetHeader(key string) string {
	return p.header.Get(key)
}

// Deletes the HTTP Header given a Header Key for this specific Thrift Transport
// It is important that you first assert the TTransport as a TalosHttpClient type
// like so:
//
// httpTrans := trans.(TalosHttpClient)
// httpTrans.DelHeader("User-Agent")
func (p *TalosHttpClient) DelHeader(key string) {
	p.header.Del(key)
}

func (p *TalosHttpClient) Open() error {
	//do nothing
	return nil
}

func (p *TalosHttpClient) IsOpen() bool {
	return p.response != nil || p.requestBuffer != nil
}

func (p *TalosHttpClient) Peek() bool {
	return p.IsOpen()
}

func (p *TalosHttpClient) Close() error {
	if p.requestBuffer != nil {
		p.requestBuffer.Reset()
		p.requestBuffer = nil
	}
	p.header = http.Header{}
	if p.response != nil && p.response.Body != nil {
		err := p.response.Body.Close()
		p.response = nil
		return err
	}
	return nil
}

func (p *TalosHttpClient) Read(buf []byte) (int, error) {
	if p.response == nil {
		return 0, thrift.NewTTransportException(thrift.NOT_OPEN,
			"Response buffer is empty, no request.")
	}
	n, err := p.response.Body.Read(buf)
	if n > 0 && (err == nil || err == io.EOF) {
		log.Debugf("read: %s", string(buf))
		return n, nil
	}
	return n, thrift.NewTTransportExceptionFromError(err)
}

func (p *TalosHttpClient) ReadByte() (c byte, err error) {
	return readByte(p.response.Body)
}

func (p *TalosHttpClient) Write(buf []byte) (int, error) {
	n, err := p.requestBuffer.Write(buf)
	return n, err
}

func (p *TalosHttpClient) WriteByte(c byte) error {
	return p.requestBuffer.WriteByte(c)
}

func (p *TalosHttpClient) WriteString(s string) (n int, err error) {
	return p.requestBuffer.WriteString(s)
}

func (p *TalosHttpClient) generateRandomId(length int) string {
	requestId, _ := uuid.NewV4()
	return requestId.String()[0:length]
}

func (p *TalosHttpClient) Flush() error {
	requestId := p.generateRandomId(RequestIdLength)
	var uri string
	if p.queryString == "" {
		uri = fmt.Sprintf("%s?id=%s", p.url.String(), requestId)
	} else {
		uri = fmt.Sprintf("%s?id=%s&%s", p.url.String(), requestId, p.queryString)
	}
	req, err := http.NewRequest("POST", uri, p.requestBuffer)
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}
	canonicalizeResource := p.canonicalizeResource(uri)

	for k, v := range *p.createHeaders() {
		log.Debugf("%s: %s", k, v)
		p.header.Add(k, v)
	}

	req.Header = p.header
	authString, err := p.authHeader(&req.Header, canonicalizeResource)
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}
	req.Header.Add(auth.HK_AUTHORIZATION, authString)
	log.Debugf("Send http request: %v", req.URL)
	response, err := p.httpClient.Do(req)
	if err != nil {
		log.Errorf("Failed to exec http request: %v", req)
		return thrift.NewTTransportExceptionFromError(err)
	}
	p.response = response
	if response.StatusCode != http.StatusOK && response.Body == nil {
		var serverTime int64
		hts := response.Header.Get(auth.HK_TIMESTAMP)
		if ts, err := strconv.Atoi(hts); err == nil {
			serverTime = int64(ts)
		} else {
			serverTime = time.Now().Unix()
		}
		log.Errorf("HTTP status: %s, failed to exec http request: %v",
			response.Status, req)
		return NewTalosTransportError(common.HttpStatusCode(int64(response.StatusCode)),
			response.Status, serverTime)
	}
	return nil
}

func (p *TalosHttpClient) canonicalizeResource(uri string) string {
	subResource := []string{"acl", "quota", "uploads", "partNumber",
		"uploadId", "storageAccessToken", "metadata"}
	parseUrl, _ := url.Parse(uri)
	result := parseUrl.Path
	queryArgs := parseUrl.Query()
	canonicalizeQuery := make([]string, 0, len(queryArgs))

	for k, _ := range queryArgs {
		if p.contains(&subResource, k) {
			canonicalizeQuery = append(canonicalizeQuery, k)
		}
	}
	if len(canonicalizeQuery) != 0 {
		i := 0
		sort.Strings(canonicalizeQuery)
		for _, v := range canonicalizeQuery {
			if i == 0 {
				result = fmt.Sprintf("%s?", result)
			} else {
				result = fmt.Sprintf("%s&", result)
			}
			values := queryArgs[v]
			if len(values) == 1 && values[0] == "" {
				result = fmt.Sprintf("%s%s", result, v)
			} else {
				result = fmt.Sprintf("%s%s=%s", result, v, values[len(values)-1])
			}
			i++
		}
	}
	return result
}

func (p *TalosHttpClient) contains(arr *[]string, target string) bool {
	for _, v := range *arr {
		if strings.EqualFold(v, target) {
			return true
		}
	}
	return false
}

func (p *TalosHttpClient) getHeader(headers *http.Header, key string) string {
	for k, v := range *headers {
		lowerKey := strings.ToLower(k)
		if strings.EqualFold(key, lowerKey) {
			return v[0]
		}
	}
	return ""
}

func (p *TalosHttpClient) createHeaders() *map[string]string {
	var _ = sha1.Size
	headers := make(map[string]string)
	headers[auth.HK_HOST] = p.url.Host
	headers[auth.HK_TIMESTAMP] = fmt.Sprintf("%d", time.Now().Unix()+p.clockOffset)
	md5c := md5.New()
	io.WriteString(md5c, p.requestBuffer.String())
	headers[auth.HK_CONTENT_MD5] = fmt.Sprintf("%x", md5c.Sum(nil))
	headers[auth.MI_DATE] = p.getDate()
	headers["Content-Type"] = "application/x-thrift-compact"
	headers["Content-Length"] = strconv.Itoa(p.requestBuffer.Len())
	headers["User-Agent"] = p.agent
	return &headers
}

func (p *TalosHttpClient) getDate() string {
	t := time.Now()
	timeStr := t.UTC().Format(time.RFC1123)
	return strings.Replace(timeStr, "UTC", "GMT", -1)
}

func (p *TalosHttpClient) authHeader(headers *http.Header,
	canonicalizeResource string) (string, error) {

	stringToSign := "POST\n"
	stringToSign = fmt.Sprintf("%s%s\n", stringToSign,
		p.getHeader(headers, "content-md5"))
	stringToSign = fmt.Sprintf("%s%s\n\n", stringToSign,
		p.getHeader(headers, "content-type"))
	stringToSign = fmt.Sprintf("%s%s", stringToSign,
		p.canonicalizeXiaomiHeaders(headers))
	stringToSign = fmt.Sprintf("%s%s", stringToSign,
		canonicalizeResource)
	mac := hmac.New(sha1.New, []byte(*p.credential.SecretKey))
	mac.Write([]byte(stringToSign))

	// check secretKeyId, format is "Service-Admin#SecretKeyId#developerId"
	// and set header attached info
	secretKeyId := p.credential.GetSecretKeyId()
	if strings.Contains(secretKeyId, auth.HK_SERVICE_ADMIN) {
		items := strings.Split(secretKeyId, auth.HK_SERVICE_MARK)
		if len(items) != 3 {
			err := fmt.Errorf("Invalid credential secretKeyId, expected: 3, "+
				"actual: %d. ", len(items))
			return "", err
		}
		// reset secretKeyId and add header attached info
		secretKeyId = items[1]
		headers.Add(auth.HK_ATTACHED_INFO, items[2])
	}
	return fmt.Sprintf("Galaxy-V3 %s:%s", secretKeyId,
		base64.StdEncoding.EncodeToString(mac.Sum(nil))), nil
}

func (p *TalosHttpClient) canonicalizeXiaomiHeaders(headers *http.Header) string {
	canonicalizedKeys := make([]string, 0, len(*headers))
	canonicalizedHeaders := make(map[string]string)
	for k, v := range *headers {
		lowerKey := strings.ToLower(k)
		if strings.Index(lowerKey, "x-xiaomi-") == 0 {
			canonicalizedKeys = append(canonicalizedKeys, lowerKey)
			canonicalizedHeaders[lowerKey] = strings.Join(v, ",")
		}
	}
	sort.Strings(canonicalizedKeys)
	result := ""
	for i := range canonicalizedKeys {
		result = fmt.Sprintf("%s%s:%s\n", result, canonicalizedKeys[i],
			canonicalizedHeaders[canonicalizedKeys[i]])
	}
	return result
}

func readByte(r io.Reader) (c byte, err error) {
	v := [1]byte{0}
	n, err := r.Read(v[0:1])
	if n > 0 && (err == nil || err == io.EOF) {
		return v[0], nil
	}
	if n > 0 && err != nil {
		return v[0], err
	}
	if err != nil {
		return 0, err
	}
	return v[0], nil
}

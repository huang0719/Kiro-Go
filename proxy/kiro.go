// Package proxy is the core proxy layer for the Kiro API.
// It handles streaming API calls to the Kiro backend and parses AWS Event Stream responses.
package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kiro-go/config"
	"kiro-go/logger"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// firstTokenTimeout 限制 HTTP 200 后等待上游首个事件的时间。
const firstTokenTimeout = 15 * time.Second

// firstTokenMaxRetries 限制首 token 超时后的重新请求次数。
const firstTokenMaxRetries = 3

// errFirstTokenTimeout 表示上游 HTTP 200 后迟迟没有返回首个事件。
var errFirstTokenTimeout = errors.New("kiro first token timeout")

// errKiroPayloadTooLarge 表示最终请求体仍超过 Kiro 上游硬限制。
var errKiroPayloadTooLarge = errors.New("kiro payload too large")

// Endpoint configuration (auto-fallback on quota exhaustion).
type kiroEndpoint struct {
	URL       string
	Origin    string
	AmzTarget string
	Name      string
}

var kiroEndpoints = []kiroEndpoint{
	{
		URL:       "https://q.us-east-1.amazonaws.com/generateAssistantResponse",
		Origin:    "AI_EDITOR",
		AmzTarget: "",
		Name:      "Kiro IDE",
	},
	{
		URL:       "https://codewhisperer.us-east-1.amazonaws.com/generateAssistantResponse",
		Origin:    "AI_EDITOR",
		AmzTarget: "AmazonCodeWhispererStreamingService.GenerateAssistantResponse",
		Name:      "CodeWhisperer",
	},
	{
		URL:       "https://q.us-east-1.amazonaws.com/generateAssistantResponse",
		Origin:    "AI_EDITOR",
		AmzTarget: "AmazonQDeveloperStreamingService.SendMessage",
		Name:      "AmazonQ",
	},
}

// Global HTTP clients, swappable at runtime to apply proxy reconfiguration without restart.
var kiroHttpStore atomic.Pointer[http.Client]
var kiroRestHttpStore atomic.Pointer[http.Client]

// proxyClientCache caches http.Client instances keyed by proxy URL for per-account proxy support.
var proxyClientCache sync.Map

func init() {
	InitKiroHttpClient("")
}

// GetClientForProxy returns an http.Client configured for the given proxy URL.
// If proxyURL is empty, returns the global kiro HTTP client.
func GetClientForProxy(proxyURL string) *http.Client {
	if proxyURL == "" {
		return kiroHttpStore.Load()
	}
	if cached, ok := proxyClientCache.Load(proxyURL); ok {
		return cached.(*http.Client)
	}
	client := &http.Client{
		Transport: buildKiroTransport(proxyURL),
	}
	proxyClientCache.Store(proxyURL, client)
	return client
}

// GetRestClientForProxy returns a rest http.Client (30s timeout) for the given proxy URL.
// If proxyURL is empty, returns the global kiro REST HTTP client.
func GetRestClientForProxy(proxyURL string) *http.Client {
	if proxyURL == "" {
		return kiroRestHttpStore.Load()
	}
	cacheKey := "rest:" + proxyURL
	if cached, ok := proxyClientCache.Load(cacheKey); ok {
		return cached.(*http.Client)
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: buildKiroTransport(proxyURL),
	}
	proxyClientCache.Store(cacheKey, client)
	return client
}

// ResolveAccountProxyURL returns the effective proxy URL for an account.
// Falls back to global config.GetProxyURL() if the account has no per-account proxy.
func ResolveAccountProxyURL(account *config.Account) string {
	if account != nil && account.ProxyURL != "" {
		return account.ProxyURL
	}
	return config.GetProxyURL()
}

// buildKiroTransport constructs an HTTP Transport with optional outbound proxy support.
func buildKiroTransport(proxyURL string) *http.Transport {
	t := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		ForceAttemptHTTP2:   true,
	}
	if proxyURL != "" {
		if u, err := url.Parse(proxyURL); err == nil {
			t.Proxy = http.ProxyURL(u)
			// Proxied connections cannot negotiate HTTP/2.
			t.ForceAttemptHTTP2 = false
		}
	} else {
		t.Proxy = http.ProxyFromEnvironment
	}
	return t
}

// InitKiroHttpClient initializes (or reinitializes) the HTTP clients used for Kiro API requests.
func InitKiroHttpClient(proxyURL string) {
	client := &http.Client{
		Transport: buildKiroTransport(proxyURL),
	}
	kiroHttpStore.Store(client)

	restClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: buildKiroTransport(proxyURL),
	}
	kiroRestHttpStore.Store(restClient)
}

// ==================== Request Structs ====================

// KiroPayload is the top-level request body sent to the Kiro API.
type KiroPayload struct {
	ConversationState struct {
		AgentContinuationId string `json:"agentContinuationId,omitempty"`
		AgentTaskType       string `json:"agentTaskType,omitempty"`
		ChatTriggerType     string `json:"chatTriggerType"`
		ConversationID      string `json:"conversationId"`
		CurrentMessage      struct {
			UserInputMessage KiroUserInputMessage `json:"userInputMessage"`
		} `json:"currentMessage"`
		History []KiroHistoryMessage `json:"history,omitempty"`
	} `json:"conversationState"`
	ProfileArn      string           `json:"profileArn,omitempty"`
	InferenceConfig *InferenceConfig `json:"inferenceConfig,omitempty"`

	// ToolNameMap maps sanitized tool names (sent to Kiro) back to the
	// original names supplied by the client. Used to restore original names
	// in tool_use responses so the client can match them to its tool registry.
	// Not serialized to the Kiro API request body.
	ToolNameMap map[string]string `json:"-"`
}

type KiroUserInputMessage struct {
	Content                 string                   `json:"content"`
	ModelID                 string                   `json:"modelId,omitempty"`
	Origin                  string                   `json:"origin"`
	Images                  []KiroImage              `json:"images,omitempty"`
	UserInputMessageContext *UserInputMessageContext `json:"userInputMessageContext,omitempty"`
}

type UserInputMessageContext struct {
	Tools       []KiroToolWrapper `json:"tools,omitempty"`
	ToolResults []KiroToolResult  `json:"toolResults,omitempty"`
}

type KiroToolWrapper struct {
	ToolSpecification struct {
		Name        string      `json:"name"`
		Description string      `json:"description"`
		InputSchema InputSchema `json:"inputSchema"`
	} `json:"toolSpecification"`
}

type InputSchema struct {
	JSON interface{} `json:"json"`
}

type KiroToolResult struct {
	ToolUseID string              `json:"toolUseId"`
	Content   []KiroResultContent `json:"content"`
	Status    string              `json:"status"`
}

type KiroResultContent struct {
	Text string `json:"text"`
}

type KiroImage struct {
	Format string `json:"format"`
	Source struct {
		Bytes string `json:"bytes"`
	} `json:"source"`
}

type KiroHistoryMessage struct {
	UserInputMessage         *KiroUserInputMessage         `json:"userInputMessage,omitempty"`
	AssistantResponseMessage *KiroAssistantResponseMessage `json:"assistantResponseMessage,omitempty"`
}

type KiroAssistantResponseMessage struct {
	Content  string        `json:"content"`
	ToolUses []KiroToolUse `json:"toolUses,omitempty"`
}

type KiroToolUse struct {
	ToolUseID string                 `json:"toolUseId"`
	Name      string                 `json:"name"`
	Input     map[string]interface{} `json:"input"`
}

type InferenceConfig struct {
	MaxTokens   int     `json:"maxTokens,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
	TopP        float64 `json:"topP,omitempty"`
}

// ==================== Stream Callbacks ====================

// KiroStreamCallback stream response callbacks
type KiroStreamCallback struct {
	OnText         func(text string, isThinking bool)
	OnToolUse      func(toolUse KiroToolUse)
	OnComplete     func(inputTokens, outputTokens int)
	OnError        func(err error)
	OnCredits      func(credits float64)
	OnContextUsage func(percentage float64)
	// OnTruncated 在流被上游中途截断（消息边界内 EOF/ErrUnexpectedEOF）时触发，
	// 上层据此把 stop_reason 标为 max_tokens，引导客户端续写。
	OnTruncated func()
}

type kiroEventFrame struct {
	eventType    string
	payloadBytes []byte
}

type kiroEventFrameResult struct {
	frame     kiroEventFrame
	done      bool
	truncated bool
	err       error
}

type kiroEventStreamState struct {
	callback             *KiroStreamCallback
	inputTokens          int
	outputTokens         int
	totalCredits         float64
	currentToolUse       *toolUseState
	lastAssistantContent string
	lastReasoningContent string
	truncated            bool
}

// ==================== API Call ====================

func setPayloadProfileArnForAccount(payload *KiroPayload, account *config.Account) {
	if payload == nil {
		return
	}

	payload.ProfileArn = strings.TrimSpace(payload.ProfileArn)
	if account != nil {
		if profileArn := strings.TrimSpace(account.ProfileArn); profileArn != "" {
			payload.ProfileArn = profileArn
		}
	}
}

// getSortedEndpoints returns endpoints ordered by user preference, with optional fallback.
func getSortedEndpoints(preferred string) []kiroEndpoint {
	fallback := config.GetEndpointFallback()

	var primary int
	switch preferred {
	case "kiro":
		primary = 0
	case "codewhisperer":
		primary = 1
	case "amazonq":
		primary = 2
	default:
		primary = 0
	}

	if !fallback {
		// No fallback: only use the selected endpoint
		return []kiroEndpoint{kiroEndpoints[primary]}
	}

	// With fallback: selected first, then others in order
	result := []kiroEndpoint{kiroEndpoints[primary]}
	for i, ep := range kiroEndpoints {
		if i != primary {
			result = append(result, ep)
		}
	}
	return result
}

// CallKiroAPI calls the Kiro streaming API, trying each configured endpoint with automatic fallback.
func CallKiroAPI(account *config.Account, payload *KiroPayload, callback *KiroStreamCallback) error {
	originalProfileArn := ""
	if payload != nil {
		originalProfileArn = payload.ProfileArn
		defer func() {
			payload.ProfileArn = originalProfileArn
		}()
	}
	setPayloadProfileArnForAccount(payload, account)

	if _, err := json.Marshal(payload); err != nil {
		return err
	}

	// Debug: dump full payload for troubleshooting upstream rejections
	if payloadJSON, err := json.Marshal(payload); err == nil {
		logger.Debugf("[KiroAPI] Request payload: %s", string(payloadJSON))
	}

	// Wrap OnToolUse to restore original tool names for the client.
	if callback != nil && callback.OnToolUse != nil && len(payload.ToolNameMap) > 0 {
		originalOnToolUse := callback.OnToolUse
		nameMap := payload.ToolNameMap
		wrapped := *callback
		wrapped.OnToolUse = func(tu KiroToolUse) {
			if original, ok := nameMap[tu.Name]; ok {
				tu.Name = original
			}
			originalOnToolUse(tu)
		}
		callback = &wrapped
	}

	if payload != nil && strings.TrimSpace(payload.ProfileArn) == "" {
		if profileArn, err := ResolveProfileArn(account); err == nil {
			payload.ProfileArn = profileArn
		} else {
			accountEmail := "<nil>"
			if account != nil {
				accountEmail = account.Email
			}
			logger.Warnf("[ProfileArn] Failed to resolve profile ARN for %s: %v", accountEmail, err)
		}
	}

	// Build endpoint list ordered by configuration.
	endpoints := getSortedEndpoints(config.GetPreferredEndpoint())

	var lastErr error
	for _, ep := range endpoints {
		err := callKiroEndpointWithFirstTokenRetry(account, payload, callback, ep)
		if err == nil {
			return nil
		}
		lastErr = err
		if errors.Is(err, errKiroPayloadTooLarge) {
			return err
		}
		if isTerminalKiroHTTPError(err) {
			return err
		}
		logger.Warnf("[KiroAPI] Endpoint %s error: %v", ep.Name, err)
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("all endpoints failed")
}

// callKiroEndpointWithFirstTokenRetry 调用单个上游端点，并在首 token 超时时重新请求。
func callKiroEndpointWithFirstTokenRetry(account *config.Account, payload *KiroPayload, callback *KiroStreamCallback, ep kiroEndpoint) error {
	for attempt := 1; attempt <= firstTokenMaxRetries; attempt++ {
		err := callKiroEndpointOnce(account, payload, callback, ep)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errFirstTokenTimeout) {
			return err
		}
		logger.Warnf("[KiroAPI] Endpoint %s first token timeout attempt %d/%d", ep.Name, attempt, firstTokenMaxRetries)
	}
	return fmt.Errorf("%w on %s after %d attempts", errFirstTokenTimeout, ep.Name, firstTokenMaxRetries)
}

// callKiroEndpointOnce 发送一次 Kiro 请求并解析流式响应。
func callKiroEndpointOnce(account *config.Account, payload *KiroPayload, callback *KiroStreamCallback, ep kiroEndpoint) error {
	payload.ConversationState.CurrentMessage.UserInputMessage.Origin = ep.Origin

	reqBody, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(reqBody) > maxPayloadBytes {
		return newKiroPayloadTooLargeError(len(reqBody))
	}

	req, err := http.NewRequest("POST", ep.URL, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}

	host := ""
	if parsedURL, parseErr := url.Parse(ep.URL); parseErr == nil {
		host = parsedURL.Host
	}
	headerValues := buildStreamingHeaderValues(account, host)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")
	if ep.AmzTarget != "" {
		req.Header.Set("X-Amz-Target", ep.AmzTarget)
	}
	applyKiroBaseHeaders(req, account, headerValues)
	req.Header.Set("x-amzn-kiro-agent-mode", "vibe")
	req.Header.Set("x-amzn-codewhisperer-optout", "true")
	req.Header.Set("Amz-Sdk-Request", "attempt=1; max=3")
	req.Header.Set("Amz-Sdk-Invocation-Id", uuid.New().String())

	resp, err := GetClientForProxy(ResolveAccountProxyURL(account)).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		logger.Warnf("[KiroAPI] Endpoint %s quota exhausted (429), trying next...", ep.Name)
		return fmt.Errorf("quota exhausted on %s", ep.Name)
	}

	if resp.StatusCode != 200 {
		errBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, ep.Name, string(errBody))
	}

	return parseEventStreamWithFirstTokenTimeout(resp.Body, callback, firstTokenTimeout)
}

// newKiroPayloadTooLargeError 生成明确的 payload 超限错误。
func newKiroPayloadTooLargeError(size int) error {
	return fmt.Errorf("%w: %d bytes exceeds limit %d bytes", errKiroPayloadTooLarge, size, maxPayloadBytes)
}

// isTerminalKiroHTTPError 判断是否为不应继续端点 fallback 的 HTTP 错误。
func isTerminalKiroHTTPError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "HTTP 401") ||
		strings.Contains(msg, "HTTP 402") ||
		strings.Contains(msg, "HTTP 403")
}

// ==================== Event Stream Parsing ====================

// parseEventStream decodes an AWS binary Event Stream response body.
func parseEventStream(body io.Reader, callback *KiroStreamCallback) error {
	state := newKiroEventStreamState(callback)
	for {
		frame, done, truncated, err := readKiroEventFrame(body)
		if err != nil {
			return err
		}
		if truncated {
			state.truncated = true
			break
		}
		if done {
			break
		}
		state.process(frame)
	}
	state.finish()
	return nil
}

// parseEventStreamWithFirstTokenTimeout 解析流，并限制首个事件的等待时间。
func parseEventStreamWithFirstTokenTimeout(body io.ReadCloser, callback *KiroStreamCallback, timeout time.Duration) error {
	state := newKiroEventStreamState(callback)
	frame, done, truncated, err := readFirstKiroEventFrame(body, timeout)
	if err != nil {
		return err
	}
	if truncated {
		state.truncated = true
		state.finish()
		return nil
	}
	if done {
		state.finish()
		return nil
	}
	state.process(frame)

	for {
		frame, done, truncated, err := readKiroEventFrame(body)
		if err != nil {
			return err
		}
		if truncated {
			state.truncated = true
			break
		}
		if done {
			break
		}
		state.process(frame)
	}
	state.finish()
	return nil
}

// newKiroEventStreamState 创建 Kiro 事件流解析状态。
func newKiroEventStreamState(callback *KiroStreamCallback) *kiroEventStreamState {
	if callback == nil {
		callback = &KiroStreamCallback{}
	}
	return &kiroEventStreamState{callback: callback}
}

// process 处理单个 Kiro 事件帧。
func (s *kiroEventStreamState) process(frame kiroEventFrame) {
	if len(frame.payloadBytes) == 0 {
		return
	}
	var event map[string]interface{}
	if err := json.Unmarshal(frame.payloadBytes, &event); err != nil {
		return
	}

	s.inputTokens, s.outputTokens = updateTokensFromEvent(event, s.inputTokens, s.outputTokens)

	switch frame.eventType {
	case "assistantResponseEvent":
		if content, ok := event["content"].(string); ok && content != "" {
			normalized := normalizeChunk(content, &s.lastAssistantContent)
			if normalized != "" && s.callback.OnText != nil {
				s.callback.OnText(normalized, false)
			}
		}
	case "reasoningContentEvent":
		if text, ok := event["text"].(string); ok && text != "" {
			normalized := normalizeChunk(text, &s.lastReasoningContent)
			if normalized != "" && s.callback.OnText != nil {
				s.callback.OnText(normalized, true)
			}
		}
	case "toolUseEvent":
		s.currentToolUse = handleToolUseEvent(event, s.currentToolUse, s.callback)
	case "meteringEvent":
		if usage, ok := event["usage"].(float64); ok {
			s.totalCredits += usage
		}
	case "contextUsageEvent":
		if pct, ok := event["contextUsagePercentage"].(float64); ok {
			if s.callback.OnContextUsage != nil {
				s.callback.OnContextUsage(pct)
			}
		}
	}
}

// finish 完成 Kiro 事件流解析并发出收尾回调。
func (s *kiroEventStreamState) finish() {
	if s.currentToolUse != nil {
		finishToolUse(s.currentToolUse, s.callback)
	}
	if s.callback.OnCredits != nil && s.totalCredits > 0 {
		s.callback.OnCredits(s.totalCredits)
	}
	if s.truncated && s.callback.OnTruncated != nil {
		s.callback.OnTruncated()
	}
	if s.callback.OnComplete != nil {
		s.callback.OnComplete(s.inputTokens, s.outputTokens)
	}
}

// readFirstKiroEventFrame 在超时时间内读取第一个 Kiro 事件帧。
func readFirstKiroEventFrame(body io.ReadCloser, timeout time.Duration) (kiroEventFrame, bool, bool, error) {
	if timeout <= 0 {
		return readKiroEventFrame(body)
	}
	ch := make(chan kiroEventFrameResult, 1)
	go func() {
		frame, done, truncated, err := readKiroEventFrame(body)
		ch <- kiroEventFrameResult{frame: frame, done: done, truncated: truncated, err: err}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-ch:
		return result.frame, result.done, result.truncated, result.err
	case <-timer.C:
		_ = body.Close()
		return kiroEventFrame{}, false, false, errFirstTokenTimeout
	}
}

// readKiroEventFrame 读取一个 AWS Event Stream 事件帧。
func readKiroEventFrame(body io.Reader) (kiroEventFrame, bool, bool, error) {
	prelude := make([]byte, 12)
	_, err := io.ReadFull(body, prelude)
	if err == io.EOF {
		return kiroEventFrame{}, true, false, nil
	}
	if err == io.ErrUnexpectedEOF {
		return kiroEventFrame{}, false, true, nil
	}
	if err != nil {
		return kiroEventFrame{}, false, false, err
	}

	totalLength := int(prelude[0])<<24 | int(prelude[1])<<16 | int(prelude[2])<<8 | int(prelude[3])
	headersLength := int(prelude[4])<<24 | int(prelude[5])<<16 | int(prelude[6])<<8 | int(prelude[7])
	if totalLength < 16 {
		return kiroEventFrame{}, false, false, nil
	}

	remaining := totalLength - 12
	msgBuf := make([]byte, remaining)
	_, err = io.ReadFull(body, msgBuf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return kiroEventFrame{}, false, true, nil
	}
	if err != nil {
		return kiroEventFrame{}, false, false, err
	}

	if headersLength > len(msgBuf)-4 {
		return kiroEventFrame{}, false, false, nil
	}

	payloadBytes := msgBuf[headersLength : len(msgBuf)-4]
	if len(payloadBytes) == 0 {
		return kiroEventFrame{}, false, false, nil
	}

	return kiroEventFrame{
		eventType:    extractEventType(msgBuf[0:headersLength]),
		payloadBytes: payloadBytes,
	}, false, false, nil
}

func updateTokensFromEvent(event map[string]interface{}, currentInputTokens, currentOutputTokens int) (int, int) {
	candidates := []map[string]interface{}{event}
	collectUsageMaps(event, &candidates)

	inputTokens := currentInputTokens
	outputTokens := currentOutputTokens

	for _, usage := range candidates {
		if usage == nil {
			continue
		}

		if v, ok := readTokenNumber(usage,
			"outputTokens", "completionTokens", "totalOutputTokens",
			"output_tokens", "completion_tokens", "total_output_tokens",
		); ok {
			outputTokens = v
		}

		if v, ok := readTokenNumber(usage,
			"inputTokens", "promptTokens", "totalInputTokens",
			"input_tokens", "prompt_tokens", "total_input_tokens",
		); ok {
			inputTokens = v
			continue
		}

		uncached, _ := readTokenNumber(usage, "uncachedInputTokens", "uncached_input_tokens")
		cacheRead, _ := readTokenNumber(usage, "cacheReadInputTokens", "cache_read_input_tokens")
		cacheWrite, _ := readTokenNumber(usage, "cacheWriteInputTokens", "cache_write_input_tokens", "cacheCreationInputTokens", "cache_creation_input_tokens")
		if uncached+cacheRead+cacheWrite > 0 {
			inputTokens = uncached + cacheRead + cacheWrite
			continue
		}

		total, ok := readTokenNumber(usage, "totalTokens", "total_tokens")
		if ok && total > 0 {
			candidateOutput := outputTokens
			if v, vok := readTokenNumber(usage,
				"outputTokens", "completionTokens", "totalOutputTokens",
				"output_tokens", "completion_tokens", "total_output_tokens",
			); vok {
				candidateOutput = v
			}
			if total-candidateOutput > 0 {
				inputTokens = total - candidateOutput
			}
		}
	}

	return inputTokens, outputTokens
}

// getContextWindowSize returns the context window size (in tokens) for a model.
//
// Per Kiro's ListAvailableModels, the 1M-token context window applies to
// Claude 4.6 and newer (sonnet-4.6, opus-4.6, opus-4.7, opus-4.8, and future
// 4.x releases), while 4.5 and earlier (opus-4.5, sonnet-4.5, sonnet-4,
// haiku-4.5) use a 200K window. This value is used to convert the upstream
// contextUsagePercentage into an absolute input-token count that clients rely
// on to decide when to compact; an undersized window under-reports tokens and
// prevents clients from compacting in time.
func getContextWindowSize(model string) int {
	if isLargeContextModel(model) {
		return 1_000_000
	}
	return 200_000
}

// largeContextMinor matches "claude-<family>-<major>.<minor>" (dot or dash form)
// and is used to classify 1M-window models by version.
var claudeVersionExtractor = regexp.MustCompile(`claude-(?:opus|sonnet|haiku)-(\d+)[.-](\d+)`)

func isLargeContextModel(model string) bool {
	m := strings.ToLower(model)
	if match := claudeVersionExtractor.FindStringSubmatch(m); match != nil {
		major, errMaj := strconv.Atoi(match[1])
		minor, errMin := strconv.Atoi(match[2])
		if errMaj == nil && errMin == nil {
			// 1M window for Claude >= 4.6 (4.6, 4.7, 4.8, ...) and any major >= 5.
			if major > 4 {
				return true
			}
			if major == 4 && minor >= 6 {
				return true
			}
			return false
		}
	}
	// Fallback substring checks for non-standard identifiers.
	for _, tag := range []string{"4.6", "4-6", "4.7", "4-7", "4.8", "4-8", "4.9", "4-9"} {
		if strings.Contains(m, tag) {
			return true
		}
	}
	return false
}

func collectUsageMaps(v interface{}, out *[]map[string]interface{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, child := range t {
			lk := strings.ToLower(k)
			if lk == "usage" || lk == "tokenusage" || lk == "token_usage" {
				if m, ok := child.(map[string]interface{}); ok {
					*out = append(*out, m)
				}
			}
			collectUsageMaps(child, out)
		}
	case []interface{}:
		for _, child := range t {
			collectUsageMaps(child, out)
		}
	}
}

func normalizeChunk(chunk string, previous *string) string {
	if chunk == "" {
		return ""
	}

	prev := *previous
	if prev == "" {
		*previous = chunk
		return chunk
	}

	if chunk == prev {
		return ""
	}

	if strings.HasPrefix(chunk, prev) {
		delta := chunk[len(prev):]
		*previous = chunk
		return delta
	}

	if strings.HasPrefix(prev, chunk) {
		return ""
	}

	maxOverlap := 0
	maxLen := len(prev)
	if len(chunk) < maxLen {
		maxLen = len(chunk)
	}
	for i := maxLen; i > 0; i-- {
		if strings.HasSuffix(prev, chunk[:i]) {
			maxOverlap = i
			break
		}
	}

	*previous = chunk
	if maxOverlap > 0 {
		return chunk[maxOverlap:]
	}

	return chunk
}

func readTokenNumber(m map[string]interface{}, keys ...string) (int, bool) {
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			continue
		}
		switch n := v.(type) {
		case float64:
			return int(n), true
		case int:
			return n, true
		case int64:
			return int(n), true
		case json.Number:
			if parsed, err := n.Int64(); err == nil {
				return int(parsed), true
			}
		case string:
			if parsed, err := strconv.Atoi(n); err == nil {
				return parsed, true
			}
			if parsed, err := strconv.ParseFloat(n, 64); err == nil {
				return int(parsed), true
			}
		}
	}
	return 0, false
}

// ==================== Tool Use Handling ====================

type toolUseState struct {
	ToolUseID   string
	Name        string
	InputBuffer strings.Builder
	GeneratedID bool
}

func handleToolUseEvent(event map[string]interface{}, current *toolUseState, callback *KiroStreamCallback) *toolUseState {
	toolUseID := firstStringField(event, "toolUseId", "toolUseID", "tool_use_id", "id")
	name := firstStringField(event, "name", "toolName", "tool_name")
	isStop := firstBoolField(event, "stop", "isStop", "done")

	if toolUseID != "" && name != "" {
		if current == nil {
			current = &toolUseState{ToolUseID: toolUseID, Name: name}
		} else if current.ToolUseID != toolUseID {
			if current.GeneratedID && current.Name == name {
				current.ToolUseID = toolUseID
				current.GeneratedID = false
			} else {
				finishToolUse(current, callback)
				current = &toolUseState{ToolUseID: toolUseID, Name: name}
			}
		}
	} else if name != "" && current == nil {
		current = &toolUseState{ToolUseID: "toolu_" + uuid.New().String(), Name: name, GeneratedID: true}
	} else if name != "" && current != nil && current.Name != name {
		finishToolUse(current, callback)
		current = &toolUseState{ToolUseID: "toolu_" + uuid.New().String(), Name: name, GeneratedID: true}
	}

	if current != nil {
		if input, ok := event["input"].(string); ok {
			current.InputBuffer.WriteString(input)
		} else if inputObj, ok := event["input"].(map[string]interface{}); ok {
			data, _ := json.Marshal(inputObj)
			current.InputBuffer.Reset()
			current.InputBuffer.Write(data)
		}
	}

	if isStop && current != nil {
		finishToolUse(current, callback)
		return nil
	}

	return current
}

func finishToolUse(state *toolUseState, callback *KiroStreamCallback) {
	if state == nil || state.Name == "" || callback == nil || callback.OnToolUse == nil {
		return
	}
	if state.ToolUseID == "" {
		state.ToolUseID = "toolu_" + uuid.New().String()
	}
	var input map[string]interface{}
	if state.InputBuffer.Len() > 0 {
		json.Unmarshal([]byte(state.InputBuffer.String()), &input)
	}
	if input == nil {
		input = make(map[string]interface{})
	}
	callback.OnToolUse(KiroToolUse{
		ToolUseID: state.ToolUseID,
		Name:      state.Name,
		Input:     input,
	})
}

func firstStringField(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

func firstBoolField(m map[string]interface{}, keys ...string) bool {
	for _, key := range keys {
		if v, ok := m[key].(bool); ok {
			return v
		}
	}
	return false
}

// extractEventType extracts the event type string from AWS Event Stream message headers.
func extractEventType(headers []byte) string {
	offset := 0
	for offset < len(headers) {
		if offset >= len(headers) {
			break
		}
		nameLen := int(headers[offset])
		offset++
		if offset+nameLen > len(headers) {
			break
		}
		name := string(headers[offset : offset+nameLen])
		offset += nameLen
		if offset >= len(headers) {
			break
		}
		valueType := headers[offset]
		offset++

		if valueType == 7 { // String
			if offset+2 > len(headers) {
				break
			}
			valueLen := int(headers[offset])<<8 | int(headers[offset+1])
			offset += 2
			if offset+valueLen > len(headers) {
				break
			}
			value := string(headers[offset : offset+valueLen])
			offset += valueLen
			if name == ":event-type" {
				return value
			}
			continue
		}

		// Skip other value types by their fixed byte widths.
		skipSizes := map[byte]int{0: 0, 1: 0, 2: 1, 3: 2, 4: 4, 5: 8, 8: 8, 9: 16}
		if valueType == 6 {
			if offset+2 > len(headers) {
				break
			}
			l := int(headers[offset])<<8 | int(headers[offset+1])
			offset += 2 + l
		} else if skip, ok := skipSizes[valueType]; ok {
			offset += skip
		} else {
			break
		}
	}
	return ""
}

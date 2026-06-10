// Package proxy: web search injection.
//
// 号池路径（Kiro 直连）不直接转发到 chat 接口，无法在上游侧做联网搜索，
// 因此这里采用「搜索 → 注入结果 → 让模型正常回答」的方式：
//  1. 检测请求是否带服务端 web_search 工具（type 以 web_search_ 开头）
//  2. 提取最后一条 user 文本作为查询词
//  3. 调 Tavily（主）/ Serper（兜底）
//  4. 把真实搜索结果作为一条 user 消息追加到 messages 尾部
//  5. 去掉 web_search 工具（上游不识别该 type，需清理）
//
// 对 Claude(messages) 与 OpenAI(chat/completions) 两种 body 都适用。
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kiro-go/config"
	"kiro-go/logger"
	"net/http"
	"strings"
	"time"
)

// 搜索 API Key（硬编码，参考 kiro-edge 实现）。
const (
	tavilyAPIKey = "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"
	tavilyURL    = "https://api.tavily.com/search"
	serperAPIKey = "74d018876214ee1eab4911192e8238a0a8456811"
	serperURL    = "https://google.serper.dev/search"
)

// searchTimeout 限制单次搜索耗时，避免拖慢整体请求。
const searchTimeout = 12 * time.Second

// WebSearchBlock 是注入到 Claude 响应里的 web_search_result 块。
type WebSearchBlock struct {
	Type    string `json:"type"`
	URL     string `json:"url"`
	Title   string `json:"title"`
	PageAge string `json:"page_age"`
}

// WebSearchMeta 携带搜索元数据，供流式/非流式响应发出
// server_tool_use / web_search_tool_result 块，并在 usage 里带上次数。
type WebSearchMeta struct {
	Query    string
	Blocks   []WebSearchBlock
	Requests int
}

// httpClientForSearch 返回带全局出站代理的 HTTP 客户端（搜索接口在海外，需复用代理）。
func httpClientForSearch() *http.Client {
	return GetRestClientForProxy(config.GetProxyURL())
}

// tavilySearch 调用 Tavily，返回格式化结果字符串。
func tavilySearch(ctx context.Context, query string) (string, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"api_key":        tavilyAPIKey,
		"query":          query,
		"search_depth":   "basic",
		"max_results":    5,
		"include_answer": true,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", tavilyURL, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClientForSearch().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("tavily error %d: %s", resp.StatusCode, truncateForLog(string(body)))
	}

	var data struct {
		Answer  string `json:"answer"`
		Results []struct {
			Title   string `json:"title"`
			URL     string `json:"url"`
			Content string `json:"content"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}

	var parts []string
	if data.Answer != "" {
		parts = append(parts, "摘要："+data.Answer+"\n")
	}
	for _, r := range data.Results {
		parts = append(parts, fmt.Sprintf("标题：%s\n链接：%s\n内容：%s\n", r.Title, r.URL, r.Content))
	}
	if len(parts) == 0 {
		return "未找到相关结果", nil
	}
	return strings.Join(parts, "\n---\n"), nil
}

// serperSearch 调用 Serper（Google），返回格式化结果字符串。
func serperSearch(ctx context.Context, query string) (string, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{"q": query, "num": 5})
	req, err := http.NewRequestWithContext(ctx, "POST", serperURL, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("X-API-KEY", serperAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClientForSearch().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("serper error %d: %s", resp.StatusCode, truncateForLog(string(body)))
	}

	var data struct {
		AnswerBox struct {
			Answer  string `json:"answer"`
			Snippet string `json:"snippet"`
		} `json:"answerBox"`
		Organic []struct {
			Title   string `json:"title"`
			Link    string `json:"link"`
			Snippet string `json:"snippet"`
		} `json:"organic"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}

	var parts []string
	if data.AnswerBox.Answer != "" {
		parts = append(parts, "摘要："+data.AnswerBox.Answer+"\n")
	} else if data.AnswerBox.Snippet != "" {
		parts = append(parts, "摘要："+data.AnswerBox.Snippet+"\n")
	}
	limit := len(data.Organic)
	if limit > 5 {
		limit = 5
	}
	for _, r := range data.Organic[:limit] {
		parts = append(parts, fmt.Sprintf("标题：%s\n链接：%s\n内容：%s", r.Title, r.Link, r.Snippet))
	}
	if len(parts) == 0 {
		return "未找到相关结果", nil
	}
	return strings.Join(parts, "\n\n---\n\n"), nil
}

// runSearch 先 Tavily 后 Serper 兜底，返回结果文本（失败返回空串）。
func runSearch(query string) string {
	ctx, cancel := context.WithTimeout(context.Background(), searchTimeout)
	defer cancel()

	result, err := tavilySearch(ctx, query)
	if err != nil {
		logger.Warnf("[web-search] Tavily 失败，切换 Serper: %v", err)
		result, err = serperSearch(ctx, query)
		if err != nil {
			logger.Warnf("[web-search] Serper 也失败，跳过搜索: %v", err)
			return ""
		}
	}
	if result == "" || result == "未找到相关结果" {
		return ""
	}
	return result
}

// parseResultBlocks 把搜索结果文本解析为 web_search_result 块数组。
// 按 \n---\n 切，抽「标题/链接」。第一段常是「摘要：...」没有链接，会被过滤。
func parseResultBlocks(result string) []WebSearchBlock {
	var blocks []WebSearchBlock
	for _, chunk := range strings.Split(result, "\n---\n") {
		var url, title string
		for _, line := range strings.Split(chunk, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "链接：") {
				url = strings.TrimSpace(strings.TrimPrefix(line, "链接："))
			} else if strings.HasPrefix(line, "标题：") {
				title = strings.TrimSpace(strings.TrimPrefix(line, "标题："))
			}
		}
		if url == "" {
			continue // 只保留真正有链接的结果，过滤摘要段
		}
		if title == "" {
			title = url
		}
		blocks = append(blocks, WebSearchBlock{Type: "web_search_result", URL: url, Title: title, PageAge: ""})
		if len(blocks) >= 5 {
			break
		}
	}
	return blocks
}

// buildSearchResultText 把搜索结果包成注入用的提示文本。
func buildSearchResultText(query, result string) string {
	return fmt.Sprintf("以下是联网搜索\"%s\"的实时结果，请基于这些信息回答用户的问题，并在合适处标注来源链接：\n\n%s", query, result)
}

func truncateForLog(s string) string {
	if len(s) > 200 {
		return s[:200]
	}
	return s
}

// ==================== 工具检测 & 查询提取 ====================

// isServerWebSearchTool 判断是否为 Anthropic 服务端联网搜索工具。
// 仅 type 以 web_search_ 开头（如 web_search_20250305）才算 —— 这是客户端明确
// 请求「请你帮我联网搜索」的信号。客户端自带的 name="WebSearch"/function 形式的
// 工具只表示该工具在本地可用，不代表这一轮要搜，绝不能据此触发搜索。
func isServerWebSearchTool(toolType string) bool {
	return strings.HasPrefix(toolType, "web_search_")
}

// claudeWantsWebSearch 判断 Claude 请求里是否要求联网搜索。
func claudeWantsWebSearch(tools []ClaudeTool) bool {
	for _, t := range tools {
		if isServerWebSearchTool(t.Type) {
			return true
		}
	}
	return false
}

// openAIWantsWebSearch 判断 OpenAI 请求里是否要求联网搜索。
func openAIWantsWebSearch(tools []OpenAITool) bool {
	for _, t := range tools {
		if isServerWebSearchTool(t.Type) {
			return true
		}
	}
	return false
}

// stripClaudeWebSearchTools 去掉服务端 web_search 工具（上游不识别该 type）。
func stripClaudeWebSearchTools(tools []ClaudeTool) []ClaudeTool {
	kept := make([]ClaudeTool, 0, len(tools))
	for _, t := range tools {
		if !isServerWebSearchTool(t.Type) {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// stripOpenAIWebSearchTools 去掉服务端 web_search 工具。
func stripOpenAIWebSearchTools(tools []OpenAITool) []OpenAITool {
	kept := make([]OpenAITool, 0, len(tools))
	for _, t := range tools {
		if !isServerWebSearchTool(t.Type) {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// queryPrefixRe 清理查询词前缀（"search for:"、"query:"、"搜索:" 等）。
const maxQueryLen = 380

// cleanQuery 去掉常见前缀并截断长度。
func cleanQuery(text string) string {
	text = strings.TrimSpace(text)
	lower := strings.ToLower(text)
	for _, p := range []string{"search for the query:", "search for:", "query:", "搜索：", "搜索:"} {
		if idx := strings.Index(lower, p); idx != -1 {
			text = strings.TrimSpace(text[idx+len(p):])
			break
		}
	}
	if len([]rune(text)) > maxQueryLen {
		text = string([]rune(text)[:maxQueryLen])
	}
	return text
}

// extractClaudeQuery 从 Claude messages 里提取最后一条 user 文本作为查询。
func extractClaudeQuery(messages []ClaudeMessage) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}
		text := claudeMessageText(messages[i].Content)
		if q := cleanQuery(text); q != "" {
			return q
		}
	}
	return ""
}

// extractOpenAIQuery 从 OpenAI messages 里提取最后一条 user 文本作为查询。
func extractOpenAIQuery(messages []OpenAIMessage) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}
		text := openAIMessageContentText(messages[i].Content)
		if q := cleanQuery(text); q != "" {
			return q
		}
	}
	return ""
}

// claudeMessageText 抽取 Claude 消息内容里的纯文本（string 或 block 数组）。
func claudeMessageText(content interface{}) string {
	if s, ok := content.(string); ok {
		return s
	}
	if blocks, ok := content.([]interface{}); ok {
		var parts []string
		for _, b := range blocks {
			block, ok := b.(map[string]interface{})
			if !ok {
				continue
			}
			if t, ok := block["text"].(string); ok {
				parts = append(parts, t)
			}
		}
		return strings.Join(parts, " ")
	}
	return ""
}

// openAIMessageContentText 抽取 OpenAI 消息内容里的纯文本。
func openAIMessageContentText(content interface{}) string {
	if s, ok := content.(string); ok {
		return s
	}
	if blocks, ok := content.([]interface{}); ok {
		var parts []string
		for _, b := range blocks {
			block, ok := b.(map[string]interface{})
			if !ok {
				continue
			}
			if t, ok := block["text"].(string); ok {
				parts = append(parts, t)
			}
		}
		return strings.Join(parts, " ")
	}
	return ""
}

// lastClaudeUserUsesBlocks 判断最后一条 user 消息是否用 block 数组形态。
func lastClaudeUserUsesBlocks(messages []ClaudeMessage) bool {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}
		_, ok := messages[i].Content.([]interface{})
		return ok
	}
	return false
}

// lastOpenAIUserUsesBlocks 判断最后一条 user 消息是否用 block 数组形态。
func lastOpenAIUserUsesBlocks(messages []OpenAIMessage) bool {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}
		_, ok := messages[i].Content.([]interface{})
		return ok
	}
	return false
}

// ==================== 注入入口 ====================

// InjectClaudeWebSearch 对 Claude 请求做联网搜索注入。
// 返回搜索元数据（用于响应里发出 server_tool_use / web_search_tool_result 块）。
// 若不需要搜索 / 提取不到查询 / 搜索失败，req 仅清理工具、返回 nil。
func InjectClaudeWebSearch(req *ClaudeRequest) *WebSearchMeta {
	if !claudeWantsWebSearch(req.Tools) {
		return nil
	}
	query := extractClaudeQuery(req.Messages)
	if query == "" {
		req.Tools = stripClaudeWebSearchTools(req.Tools)
		return nil
	}

	result := runSearch(query)
	req.Tools = stripClaudeWebSearchTools(req.Tools)
	if result == "" {
		return nil
	}

	useBlocks := lastClaudeUserUsesBlocks(req.Messages)
	text := buildSearchResultText(query, result)
	var content interface{}
	if useBlocks {
		content = []interface{}{map[string]interface{}{"type": "text", "text": text}}
	} else {
		content = text
	}
	req.Messages = append(req.Messages, ClaudeMessage{Role: "user", Content: content})

	blocks := parseResultBlocks(result)
	logger.Infof("[web-search claude] 注入搜索结果 query=%q len=%d blocks=%d", query, len(result), len(blocks))
	return &WebSearchMeta{Query: query, Blocks: blocks, Requests: 1}
}

// InjectOpenAIWebSearch 对 OpenAI 请求做联网搜索注入。
func InjectOpenAIWebSearch(req *OpenAIRequest) *WebSearchMeta {
	if !openAIWantsWebSearch(req.Tools) {
		return nil
	}
	query := extractOpenAIQuery(req.Messages)
	if query == "" {
		req.Tools = stripOpenAIWebSearchTools(req.Tools)
		return nil
	}

	result := runSearch(query)
	req.Tools = stripOpenAIWebSearchTools(req.Tools)
	if result == "" {
		return nil
	}

	useBlocks := lastOpenAIUserUsesBlocks(req.Messages)
	text := buildSearchResultText(query, result)
	var content interface{}
	if useBlocks {
		content = []interface{}{map[string]interface{}{"type": "text", "text": text}}
	} else {
		content = text
	}
	req.Messages = append(req.Messages, OpenAIMessage{Role: "user", Content: content})

	blocks := parseResultBlocks(result)
	logger.Infof("[web-search openai] 注入搜索结果 query=%q len=%d blocks=%d", query, len(result), len(blocks))
	return &WebSearchMeta{Query: query, Blocks: blocks, Requests: 1}
}

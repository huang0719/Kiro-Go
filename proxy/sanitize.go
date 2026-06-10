package proxy

import (
	"regexp"
	"strings"
)

// 脱敏：客户端不允许收到任何内部服务标识（Kiro / CodeWhisperer / AWS / ARN 等）。
// 这些替换只作用于「发给客户端」的错误消息，服务端日志与管理后台仍保留原文便于排查。

var (
	// 端点名需整体替换为中性词，必须在通用 Kiro/Amazon 替换之前处理，否则
	// "Kiro IDE" 会被逐词换成 "Claude IDE"（Claude 并无 IDE，既不通顺又露馅）。
	reEndpointKiroIDE = regexp.MustCompile(`(?i)\bKiro\s*IDE\b`)
	reEndpointAmazonQ = regexp.MustCompile(`(?i)\bAmazon\s*Q\b`)

	reKiroIDE       = regexp.MustCompile(`(?i)\bKiroIDE\b`)
	reKiro          = regexp.MustCompile(`(?i)\bKiro\b`)
	reCodeWhisperer = regexp.MustCompile(`(?i)\bCodeWhisperer\b`)
	reAmazon        = regexp.MustCompile(`(?i)\bAmazon\b`)
	reAWS           = regexp.MustCompile(`(?i)\bAWS\b`)
	reARN           = regexp.MustCompile(`arn:aws:[^"\s,}]+`)
	reProfileArn    = regexp.MustCompile(`(?i)\bprofileArn\b`)
	reAmazonHost    = regexp.MustCompile(`(?i)[a-z0-9.-]+\.amazonaws\.com`)
	reMultiSpace    = regexp.MustCompile(`\s+`)
)

// sanitizeClientError 移除错误消息中的内部服务标识，返回可安全暴露给客户端的文本。
func sanitizeClientError(msg string) string {
	if msg == "" {
		return "service error"
	}
	s := msg
	// 1. 端点名整体替换为中性词（必须最先做）。
	s = reEndpointKiroIDE.ReplaceAllString(s, "upstream")
	s = reEndpointAmazonQ.ReplaceAllString(s, "upstream")
	s = reCodeWhisperer.ReplaceAllString(s, "upstream")
	// 2. 残留的内部标识词。
	s = reKiroIDE.ReplaceAllString(s, "Claude")
	s = reKiro.ReplaceAllString(s, "Claude")
	s = reARN.ReplaceAllString(s, "[redacted]")
	s = reProfileArn.ReplaceAllString(s, "profile")
	s = reAmazonHost.ReplaceAllString(s, "upstream")
	s = reAmazon.ReplaceAllString(s, "")
	s = reAWS.ReplaceAllString(s, "")
	s = reMultiSpace.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// Package pool 账号池管理
// 实现轮询负载均衡、错误冷却、Token 刷新
package pool

import (
	"kiro-go/config"
	"kiro-go/logger"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const tokenRefreshSkewSeconds int64 = 120
const accountAcquireWait = 5 * time.Second

// AccountPool 账号池
type AccountPool struct {
	mu            sync.RWMutex
	cond          *sync.Cond
	accounts      []config.Account
	totalAccounts int
	currentIndex  uint64
	cooldowns     map[string]time.Time       // 账号冷却时间
	errorCounts   map[string]int             // 连续错误计数
	modelLists    map[string]map[string]bool // accountID → set of modelIDs (from ListAvailableModels)
	inFlight      map[string]int             // accountID → current in-flight requests
}

var (
	pool     *AccountPool
	poolOnce sync.Once
)

// GetPool 获取全局账号池单例
func GetPool() *AccountPool {
	poolOnce.Do(func() {
		pool = &AccountPool{
			cooldowns:   make(map[string]time.Time),
			errorCounts: make(map[string]int),
			modelLists:  make(map[string]map[string]bool),
			inFlight:    make(map[string]int),
		}
		pool.cond = sync.NewCond(&pool.mu)
		pool.Reload()
	})
	return pool
}

// Reload rebuilds the weighted account list from config.
// Weight <= 1 → 1 entry; weight >= 2 → weight entries.
// Over-quota accounts are dropped unless either the per-account upstream
// Overages switch (OverageStatus=ENABLED) or the global AllowOverUsage
// setting permits over-quota routing.
func (p *AccountPool) Reload() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureRuntimeLocked()
	enabled := config.GetEnabledAccounts()
	allowOverUsage := config.GetAllowOverUsage()
	var weighted []config.Account
	for _, a := range enabled {
		if isQuotaBlocked(a, allowOverUsage) {
			continue
		}
		w := effectiveWeight(a.Weight)
		for j := 0; j < w; j++ {
			weighted = append(weighted, a)
		}
	}
	p.accounts = weighted
	p.totalAccounts = len(enabled)
	active := make(map[string]bool)
	for _, a := range weighted {
		active[a.ID] = true
	}
	for id := range p.inFlight {
		if !active[id] {
			delete(p.inFlight, id)
		}
	}
	p.cond.Broadcast()
}

// GetNext 获取下一个可用账号（加权轮询）
func (p *AccountPool) GetNext() *config.Account {
	return p.GetNextExcluding(nil)
}

// GetNextExcluding 获取下一个可用账号，优先选择无并发账号；满载时等待排队。
func (p *AccountPool) GetNextExcluding(excluded map[string]bool) *config.Account {
	return p.acquireAccount("", excluded)
}

// SetModelList 缓存账号支持的模型集合（由 handler 在刷新后调用）
func (p *AccountPool) SetModelList(accountID string, modelIDs []string) {
	set := make(map[string]bool, len(modelIDs))
	for _, id := range modelIDs {
		set[strings.ToLower(strings.TrimSpace(id))] = true
	}
	p.mu.Lock()
	p.modelLists[accountID] = set
	p.mu.Unlock()
}

// GetModelList 返回该账号缓存的模型 ID 列表（供 admin API 使用）。
// 若尚无缓存则返回空切片。
func (p *AccountPool) GetModelList(accountID string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	set, ok := p.modelLists[accountID]
	if !ok || len(set) == 0 {
		return []string{}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return ids
}

// accountHasModel 检查账号是否支持指定模型。
// 若该账号尚无模型列表（冷启动），视为支持所有模型。
func (p *AccountPool) accountHasModel(accountID, model string) bool {
	list, ok := p.modelLists[accountID]
	if !ok || len(list) == 0 {
		return true // 冷启动：列表未就绪，乐观放行
	}
	return list[strings.ToLower(strings.TrimSpace(model))]
}

// GetNextForModel 获取下一个支持指定模型的可用账号。
// model 应为去掉 thinking 后缀的实际模型名。
// 若无账号有该模型列表数据，行为与 GetNext 相同（乐观路由）。
func (p *AccountPool) GetNextForModel(model string) *config.Account {
	return p.GetNextForModelExcluding(model, nil)
}

// GetNextForModelExcluding 获取支持指定模型的账号，优先空闲账号；满载时等待排队。
func (p *AccountPool) GetNextForModelExcluding(model string, excluded map[string]bool) *config.Account {
	return p.acquireAccount(model, excluded)
}

// acquireAccount 按最少 in-flight 优先的顺序占用账号。
func (p *AccountPool) acquireAccount(model string, excluded map[string]bool) *config.Account {
	deadline := time.Now().Add(accountAcquireWait)
	waiting := false
	for {
		p.mu.Lock()
		p.ensureRuntimeLocked()
		acc, blockedByBusy := p.selectAccountLocked(model, excluded)
		if acc != nil {
			before := p.inFlight[acc.ID]
			p.inFlight[acc.ID] = before + 1
			maxConcurrent := config.GetAccountMaxConcurrent()
			snapshot := p.loadSnapshotLocked(maxConcurrent)
			p.mu.Unlock()
			logger.Infof("[LoadBalance] acquired account=%s id=%s model=%s inFlight=%d/%d queued=%v load=[%s]",
				accountLabel(acc), acc.ID, modelLabel(model), before+1, maxConcurrent, waiting, snapshot)
			return acc
		}
		if !blockedByBusy || time.Now().After(deadline) {
			snapshot := p.loadSnapshotLocked(config.GetAccountMaxConcurrent())
			p.mu.Unlock()
			if blockedByBusy {
				logger.Warnf("[LoadBalance] queue timeout model=%s wait=%s load=[%s]", modelLabel(model), accountAcquireWait, snapshot)
			} else {
				logger.Warnf("[LoadBalance] no eligible account model=%s load=[%s]", modelLabel(model), snapshot)
			}
			return nil
		}
		if !waiting {
			logger.Warnf("[LoadBalance] all eligible accounts busy, queueing model=%s wait<=%s load=[%s]",
				modelLabel(model), time.Until(deadline).Round(time.Millisecond), p.loadSnapshotLocked(config.GetAccountMaxConcurrent()))
			waiting = true
		}
		p.waitLocked(deadline)
		logger.Infof("[LoadBalance] queue wake model=%s remaining=%s load=[%s]",
			modelLabel(model), time.Until(deadline).Round(time.Millisecond), p.loadSnapshotLocked(config.GetAccountMaxConcurrent()))
		p.mu.Unlock()
	}
}

// selectAccountLocked 选择 in-flight 最少且未满并发的账号；同分按轮询顺序。
func (p *AccountPool) selectAccountLocked(model string, excluded map[string]bool) (*config.Account, bool) {
	if len(p.accounts) == 0 {
		return nil, false
	}

	allowOverUsage := config.GetAllowOverUsage()
	maxConcurrent := config.GetAccountMaxConcurrent()
	now := time.Now()
	n := len(p.accounts)
	seen := make(map[string]bool)
	blockedByBusy := false
	var best *config.Account
	bestIndex := -1
	bestRunning := maxConcurrent + 1
	start := int(atomic.LoadUint64(&p.currentIndex) % uint64(n))

	for i := 0; i < n; i++ {
		idx := (start + i) % n
		acc := &p.accounts[idx]
		if seen[acc.ID] {
			continue
		}
		seen[acc.ID] = true
		if !p.accountEligibleLocked(acc, model, excluded, allowOverUsage, now) {
			continue
		}

		running := p.inFlight[acc.ID]
		if running >= maxConcurrent {
			blockedByBusy = true
			continue
		}
		if best == nil || running < bestRunning {
			best = acc
			bestIndex = idx
			bestRunning = running
			if running == 0 {
				break
			}
		}
	}
	if best != nil {
		atomic.StoreUint64(&p.currentIndex, uint64(bestIndex+1))
	}
	return best, blockedByBusy
}

// accountEligibleLocked 判断账号是否满足模型、冷却、token、额度等基础条件。
func (p *AccountPool) accountEligibleLocked(acc *config.Account, model string, excluded map[string]bool, allowOverUsage bool, now time.Time) bool {
	if acc == nil {
		return false
	}
	if excluded != nil && excluded[acc.ID] {
		return false
	}
	if model != "" && !p.accountHasModel(acc.ID, model) {
		return false
	}
	if cooldown, ok := p.cooldowns[acc.ID]; ok && now.Before(cooldown) {
		return false
	}
	if acc.ExpiresAt > 0 && now.Unix() > acc.ExpiresAt-tokenRefreshSkewSeconds {
		return false
	}
	return !isQuotaBlocked(*acc, allowOverUsage)
}

// waitLocked 在账号全忙时短暂排队等待释放信号。
func (p *AccountPool) waitLocked(deadline time.Time) {
	wait := time.Until(deadline)
	if wait <= 0 {
		return
	}
	timer := time.AfterFunc(wait, func() {
		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
	})
	p.cond.Wait()
	timer.Stop()
}

// GetByID 根据 ID 获取账号
func (p *AccountPool) GetByID(id string) *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			return &p.accounts[i]
		}
	}
	return nil
}

// RecordSuccess 记录请求成功，清除冷却
func (p *AccountPool) RecordSuccess(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureRuntimeLocked()
	p.releaseAccountLocked(id)
	delete(p.cooldowns, id)
	p.errorCounts[id] = 0
	p.cond.Broadcast()
}

// RecordError 记录请求错误，设置冷却
func (p *AccountPool) RecordError(id string, isQuotaError bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureRuntimeLocked()
	p.releaseAccountLocked(id)

	p.errorCounts[id]++

	if isQuotaError {
		// 配额错误，冷却 1 小时
		p.cooldowns[id] = time.Now().Add(time.Hour)
	} else if p.errorCounts[id] >= 3 {
		// 连续 3 次错误，冷却 1 分钟
		p.cooldowns[id] = time.Now().Add(time.Minute)
	}
	p.cond.Broadcast()
}

// Release 释放已占用账号，用于禁用、人工中止等不走成功/失败统计的路径。
func (p *AccountPool) Release(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureRuntimeLocked()
	p.releaseAccountLocked(id)
	p.cond.Broadcast()
}

// ensureRuntimeLocked 初始化运行期负载字段，兼容测试里手工构造的账号池。
func (p *AccountPool) ensureRuntimeLocked() {
	if p.cond == nil {
		p.cond = sync.NewCond(&p.mu)
	}
	if p.cooldowns == nil {
		p.cooldowns = make(map[string]time.Time)
	}
	if p.errorCounts == nil {
		p.errorCounts = make(map[string]int)
	}
	if p.modelLists == nil {
		p.modelLists = make(map[string]map[string]bool)
	}
	if p.inFlight == nil {
		p.inFlight = make(map[string]int)
	}
}

// releaseAccountLocked 释放账号 in-flight 计数。
func (p *AccountPool) releaseAccountLocked(id string) {
	if id == "" {
		return
	}
	before := p.inFlight[id]
	if before <= 0 {
		return
	}
	if p.inFlight[id] <= 1 {
		delete(p.inFlight, id)
		logger.Infof("[LoadBalance] released account id=%s inFlight=%d->0 load=[%s]",
			id, before, p.loadSnapshotLocked(config.GetAccountMaxConcurrent()))
		return
	}
	p.inFlight[id]--
	logger.Infof("[LoadBalance] released account id=%s inFlight=%d->%d load=[%s]",
		id, before, p.inFlight[id], p.loadSnapshotLocked(config.GetAccountMaxConcurrent()))
}

// loadSnapshotLocked 返回当前账号负载快照，调用方必须持有 p.mu。
func (p *AccountPool) loadSnapshotLocked(maxConcurrent int) string {
	seen := make(map[string]bool)
	parts := make([]string, 0, len(p.accounts))
	for i := range p.accounts {
		acc := &p.accounts[i]
		if seen[acc.ID] {
			continue
		}
		seen[acc.ID] = true
		parts = append(parts, accountLabel(acc)+":"+intString(p.inFlight[acc.ID])+"/"+intString(maxConcurrent))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

// accountLabel 返回日志使用的账号标识。
func accountLabel(acc *config.Account) string {
	if acc == nil {
		return "unknown"
	}
	if strings.TrimSpace(acc.Email) != "" {
		return acc.Email
	}
	return acc.ID
}

// modelLabel 返回日志使用的模型标识。
func modelLabel(model string) string {
	if strings.TrimSpace(model) == "" {
		return "any"
	}
	return model
}

// intString 将整数转换为日志字符串。
func intString(v int) string {
	return strconv.Itoa(v)
}

// IsAuthFailure reports whether an error indicates the refresh token / credentials
// have been revoked or invalidated upstream (401, 403 with auth markers, etc.).
// These accounts cannot be recovered automatically and must be re-authenticated.
func IsAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	lower := strings.ToLower(msg)

	// Match HTTP status codes only when they appear as standalone tokens to avoid
	// false positives from arbitrary digits in the error body (e.g. request IDs).
	if hasStatusToken(msg, "401") || hasStatusToken(msg, "403") {
		return true
	}
	if strings.Contains(lower, "bad credentials") ||
		strings.Contains(lower, "invalid_grant") ||
		strings.Contains(lower, "invalid grant") ||
		strings.Contains(lower, "invalid_token") ||
		strings.Contains(lower, "invalid token") ||
		strings.Contains(lower, "token expired") ||
		strings.Contains(lower, "token has expired") ||
		strings.Contains(lower, "unauthorized") {
		return true
	}
	return false
}

// hasStatusToken returns true when status appears in s with non-digit boundaries
// on both sides, so "401" matches "HTTP 401 from ..." but not "request_401abc".
func hasStatusToken(s, status string) bool {
	for {
		idx := strings.Index(s, status)
		if idx < 0 {
			return false
		}
		leftOK := idx == 0 || !isDigit(s[idx-1])
		rightIdx := idx + len(status)
		rightOK := rightIdx >= len(s) || !isDigit(s[rightIdx])
		if leftOK && rightOK {
			return true
		}
		s = s[idx+len(status):]
	}
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// IsSuspensionError reports whether the error indicates the account has been
// temporarily suspended by upstream or has no available Kiro profile.
// Unlike auth failures (revoked credentials), these may be transient, but
// the account should be disabled until an operator re-enables it.
func IsSuspensionError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "temporarily_suspended") ||
		strings.Contains(lower, "temporarily suspended") ||
		strings.Contains(lower, "no available kiro profile")
}

// DisableAccount marks an account as disabled (auth revoked / unrecoverable),
// removes it from the in-memory pool so subsequent requests skip it, and
// persists the change via config.SetAccountBanStatus.
func (p *AccountPool) DisableAccount(id, reason string) {
	if err := config.SetAccountBanStatus(id, "DISABLED", reason); err != nil {
		// best effort — even if persistence fails, drop it from memory
		_ = err
	}
	p.mu.Lock()
	p.ensureRuntimeLocked()
	p.releaseAccountLocked(id)
	// Long cooldown as a safety net in case Reload races
	p.cooldowns[id] = time.Now().Add(24 * time.Hour)
	p.mu.Unlock()
	p.Reload()
}

// MarkOverLimit marks an account as over usage limit (after a 402 / OVERAGE response).
// With the upstream OverageStatus model, the live status is refreshed via
// FetchOverageStatus from the request handler; here we just cooldown briefly so
// the next attempt picks a different account, then reload.
func (p *AccountPool) MarkOverLimit(id string) {
	p.mu.Lock()
	p.ensureRuntimeLocked()
	p.releaseAccountLocked(id)
	p.cooldowns[id] = time.Now().Add(time.Hour)
	p.mu.Unlock()
	p.Reload()
}

// UpdateToken 更新账号 Token
func (p *AccountPool) UpdateToken(id, accessToken, refreshToken string, expiresAt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			p.accounts[i].AccessToken = accessToken
			if refreshToken != "" {
				p.accounts[i].RefreshToken = refreshToken
			}
			p.accounts[i].ExpiresAt = expiresAt
		}
	}
}

// Count 返回账号总数
func (p *AccountPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.totalAccounts > 0 {
		return p.totalAccounts
	}

	seen := make(map[string]bool)
	for _, acc := range p.accounts {
		seen[acc.ID] = true
	}
	return len(seen)
}

// AvailableCount 返回可用账号数
func (p *AccountPool) AvailableCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	count := 0
	seen := make(map[string]bool)
	for _, acc := range p.accounts {
		if seen[acc.ID] {
			continue
		}
		seen[acc.ID] = true
		if cooldown, ok := p.cooldowns[acc.ID]; ok && now.Before(cooldown) {
			continue
		}
		count++
	}
	return count
}

// UpdateStats 更新账号统计
func (p *AccountPool) UpdateStats(id string, tokens int, credits float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var updated bool
	var requestCount, errorCount, totalTokens int
	var totalCredits float64
	var lastUsed int64
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			if !updated {
				p.accounts[i].RequestCount++
				p.accounts[i].TotalTokens += tokens
				p.accounts[i].TotalCredits += credits
				p.accounts[i].LastUsed = time.Now().Unix()

				requestCount = p.accounts[i].RequestCount
				errorCount = p.accounts[i].ErrorCount
				totalTokens = p.accounts[i].TotalTokens
				totalCredits = p.accounts[i].TotalCredits
				lastUsed = p.accounts[i].LastUsed
				updated = true
				continue
			}
			p.accounts[i].RequestCount = requestCount
			p.accounts[i].ErrorCount = errorCount
			p.accounts[i].TotalTokens = totalTokens
			p.accounts[i].TotalCredits = totalCredits
			p.accounts[i].LastUsed = lastUsed
		}
	}
	if updated {
		go config.UpdateAccountStats(id, requestCount, errorCount, totalTokens, totalCredits, lastUsed)
	}
}

// GetAllAccounts 获取所有账号副本
func (p *AccountPool) GetAllAccounts() []config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]config.Account, len(p.accounts))
	copy(result, p.accounts)
	return result
}

func isOverUsageLimit(acc config.Account) bool {
	return acc.UsageLimit > 0 && acc.UsageCurrent >= acc.UsageLimit
}

// isQuotaBlocked reports whether an over-quota account should be skipped:
// the per-account upstream Overages switch (OverageStatus=ENABLED) and the
// global allowOverUsage setting are the two ways to keep it routable.
func isQuotaBlocked(acc config.Account, allowOverUsage bool) bool {
	return isOverUsageLimit(acc) && !isUpstreamOverageEnabled(acc) && !allowOverUsage
}

// isUpstreamOverageEnabled reports whether the upstream Overages switch is ON for this account.
// "ENABLED" → true; anything else (DISABLED, UNKNOWN, empty) → false.
func isUpstreamOverageEnabled(acc config.Account) bool {
	return strings.EqualFold(acc.OverageStatus, "ENABLED")
}

func effectiveWeight(weight int) int {
	if weight < 1 {
		return 1
	}
	return weight
}

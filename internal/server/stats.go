// stats.go 进程内请求统计环形缓冲，供 /stats 与看板页面读取。
// 只存内存、不落盘：重启即清空，符合"本地看看"的定位。
package server

import (
	"sync"
	"time"
)

// statsCap 环形缓冲保留的最近请求条数。
const statsCap = 500

// reqRecord 单条请求记录（对外 JSON 结构）。
type reqRecord struct {
	Seq      int64   `json:"seq"`
	Time     string  `json:"time"` // HH:MM:SS
	Model    string  `json:"model"`
	Mode     string  `json:"mode"` // stream | sync
	Status   int     `json:"status"`
	UID      string  `json:"uid"`
	TTFBMs   float64 `json:"ttfb_ms"`
	Tokens   int     `json:"tokens"` // -1 = usage 缺失
	TokensOK bool    `json:"tokens_ok"`
	TokPerS  float64 `json:"tok_per_s"`
	TotalMs  float64 `json:"total_ms"`
	OK       bool    `json:"ok"`
}

// modelAgg 按模型聚合的用量。
type modelAgg struct {
	Model    string  `json:"model"`
	Reqs     int64   `json:"reqs"`
	Tokens   int64   `json:"tokens"`
	TokPerS  float64 `json:"tok_per_s"` // 该模型累计 tok/s（按总 token / 总生成时长）
	GenMs    float64 `json:"gen_ms"`    // 累计生成时长（total - ttfb）
	AvgTTFB  float64 `json:"avg_ttfb_ms"`
	Errors   int64   `json:"errors"`
	genAccum float64 // 内部：用于重算均值
	ttfbAcc  float64
}

// statsStore 环形缓冲 + 聚合计数器。
type statsStore struct {
	mu      sync.RWMutex
	ring    []reqRecord
	next    int  // 下一个写入位置
	filled  bool // 是否已绕回
	seq     int64
	total   int64 // 累计请求数（含被挤出环形缓冲的）
	okCount int64
	errTot  int64
	tokTot  int64
	genMs   float64 // 累计生成时长（total - ttfb），用于总 tok/s
	models  map[string]*modelAgg
}

var stats = newStatsStore()

func newStatsStore() *statsStore {
	return &statsStore{
		ring:   make([]reqRecord, statsCap),
		models: map[string]*modelAgg{},
	}
}

// add 写入一条记录并更新聚合。
func (s *statsStore) add(r reqRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	r.Seq = s.seq
	s.total++
	if r.OK {
		s.okCount++
	} else {
		s.errTot++
	}
	if r.TokensOK && r.Tokens > 0 {
		s.tokTot += int64(r.Tokens)
	}
	gen := r.TotalMs - r.TTFBMs
	if gen < 0 {
		gen = 0
	}
	s.genMs += gen

	m := s.models[r.Model]
	if m == nil {
		m = &modelAgg{Model: r.Model}
		s.models[r.Model] = m
	}
	m.Reqs++
	if r.TokensOK && r.Tokens > 0 {
		m.Tokens += int64(r.Tokens)
	}
	m.GenMs += gen
	m.genAccum += gen
	m.ttfbAcc += r.TTFBMs
	m.AvgTTFB = m.ttfbAcc / float64(m.Reqs)
	if r.TokensOK && m.GenMs > 0 {
		m.TokPerS = float64(m.Tokens) / (m.GenMs / 1000)
	}
	if !r.OK {
		m.Errors++
	}

	s.ring[s.next] = r
	s.next = (s.next + 1) % statsCap
	if s.next == 0 {
		s.filled = true
	}
}

// snapshot 返回汇总 + 最近请求（新→旧）+ 按模型聚合。
func (s *statsStore) snapshot() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n := s.next
	if s.filled {
		n = statsCap
	}
	recent := make([]reqRecord, 0, n)
	for i := 0; i < n; i++ {
		// 从最新往回读
		idx := (s.next - 1 - i + statsCap) % statsCap
		recent = append(recent, s.ring[idx])
	}

	models := make([]modelAgg, 0, len(s.models))
	for _, m := range s.models {
		c := *m
		models = append(models, c)
	}

	totalTokPerS := 0.0
	if s.genMs > 0 {
		totalTokPerS = float64(s.tokTot) / (s.genMs / 1000)
	}
	avgTokPerS := 0.0
	if n > 0 {
		sum := 0.0
		cnt := 0
		for _, r := range recent {
			if r.TokensOK && r.TokPerS > 0 {
				sum += r.TokPerS
				cnt++
			}
		}
		if cnt > 0 {
			avgTokPerS = sum / float64(cnt)
		}
	}

	return map[string]any{
		"summary": map[string]any{
			"total":      s.total,
			"ok":         s.okCount,
			"errors":     s.errTot,
			"tokens":     s.tokTot,
			"tok_per_s":  totalTokPerS,
			"avg_tok_ps": avgTokPerS,
			"gen_ms":     s.genMs,
		},
		"recent": recent,
		"models": models,
		"now":    time.Now().Format("15:04:05"),
	}
}

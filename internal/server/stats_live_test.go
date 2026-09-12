package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"workbuddy2api/internal/pool"
)

// TestStatsStoreAggregationLive 验证 add → snapshot 的聚合数学。
func TestStatsStoreAggregationLive(t *testing.T) {
	s := newStatsStore()
	// 100 tok / 2s 生成时长 → 50 tok/s
	s.add(reqRecord{Model: "m1", Mode: "stream", Status: 200, OK: true,
		Tokens: 100, TokensOK: true, TTFBMs: 200, TotalMs: 2200, TokPerS: 50})
	// 200 tok / 1s → 200 tok/s
	s.add(reqRecord{Model: "m1", Mode: "stream", Status: 200, OK: true,
		Tokens: 200, TokensOK: true, TTFBMs: 100, TotalMs: 1100, TokPerS: 200})
	// 失败请求，无 usage
	s.add(reqRecord{Model: "m2", Mode: "sync", Status: 429, OK: false, Tokens: -1})

	snap := s.snapshot()
	sum := snap["summary"].(map[string]any)
	if got := sum["total"].(int64); got != 3 {
		t.Fatalf("total=%d want 3", got)
	}
	if got := sum["ok"].(int64); got != 2 {
		t.Fatalf("ok=%d want 2", got)
	}
	if got := sum["tokens"].(int64); got != 300 {
		t.Fatalf("tokens=%d want 300", got)
	}
	// 总生成时长 = (2200-200)+(1100-100) = 3000ms → 300tok/3s = 100 tok/s
	if got := sum["tok_per_s"].(float64); got < 99 || got > 101 {
		t.Fatalf("tok_per_s=%v want ~100", got)
	}
	// recent 必须新→旧
	recent := snap["recent"].([]reqRecord)
	if recent[0].Model != "m2" {
		t.Fatalf("recent[0]=%s want m2 (newest first)", recent[0].Model)
	}
	if recent[0].Seq != 3 || recent[1].Seq != 2 || recent[2].Seq != 1 {
		t.Fatalf("seq order wrong: %d %d %d", recent[0].Seq, recent[1].Seq, recent[2].Seq)
	}
}

// TestStatsRingWrapLive 验证环形缓冲写满后正确绕回且不 panic。
func TestStatsRingWrapLive(t *testing.T) {
	s := newStatsStore()
	for i := 0; i < statsCap+50; i++ {
		s.add(reqRecord{Model: "m", Status: 200, OK: true, Tokens: 1, TokensOK: true, TotalMs: 100})
	}
	snap := s.snapshot()
	recent := snap["recent"].([]reqRecord)
	if len(recent) != statsCap {
		t.Fatalf("recent len=%d want %d", len(recent), statsCap)
	}
	if recent[0].Seq != int64(statsCap+50) {
		t.Fatalf("newest seq=%d want %d", recent[0].Seq, statsCap+50)
	}
	if sum := snap["summary"].(map[string]any); sum["total"].(int64) != int64(statsCap+50) {
		t.Fatalf("total=%v want %d", sum["total"], statsCap+50)
	}
}

// TestStatsEndpointNoAuthLive 验证 /stats 与 / 无需鉴权可访问（本地看板定位）。
func TestStatsEndpointNoAuthLive(t *testing.T) {
	h := NewHandler(Config{Pool: pool.New(""), APIKey: "secret"}) // 配了 key 但看板端点不应校验
	for _, tc := range []struct {
		path string
		want int
	}{
		{"/stats", 200},
		{"/", 200},
		{"/nope", 404},
		{"/status", 401}, // 原有端点仍要求鉴权
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s = %d want %d", tc.path, rec.Code, tc.want)
		}
	}
}

// TestCheckinEndpointLive 验证按钮语义：未注入→501；注入→被调用。
func TestCheckinEndpointLive(t *testing.T) {
	// 未注入
	h := NewHandler(Config{Pool: pool.New("")})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/checkin", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("no CheckinFunc: got %d want 501", rec.Code)
	}

	// 已注入：确认真的被触发
	called := 0
	h2 := NewHandler(Config{Pool: pool.New(""), CheckinFunc: func() bool { called++; return true }})
	rec2 := httptest.NewRecorder()
	h2.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/checkin", nil))
	if rec2.Code != 200 || called != 1 {
		t.Fatalf("checkin: code=%d called=%d want 200/1", rec2.Code, called)
	}

	// 已跑过（CheckinFunc 返回 false）→ 200 且带 skipped 标记，不报错
	called2 := 0
	h3 := NewHandler(Config{Pool: pool.New(""), CheckinFunc: func() bool { called2++; return false }})
	rec3b := httptest.NewRecorder()
	h3.ServeHTTP(rec3b, httptest.NewRequest(http.MethodPost, "/checkin", nil))
	if rec3b.Code != 200 || called2 != 1 {
		t.Fatalf("checkin(already): code=%d called=%d want 200/1", rec3b.Code, called2)
	}
	var skippedBody map[string]any
	if err := json.Unmarshal(rec3b.Body.Bytes(), &skippedBody); err != nil {
		t.Fatalf("checkin(already) json: %v", err)
	}
	if skippedBody["skipped"] != true {
		t.Fatalf("checkin(already) skipped=%v want true", skippedBody["skipped"])
	}

	// /stats 应上报 checkin_enabled
	rec3 := httptest.NewRecorder()
	h2.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var body map[string]any
	if err := json.Unmarshal(rec3.Body.Bytes(), &body); err != nil {
		t.Fatalf("stats json: %v", err)
	}
	if body["checkin_enabled"] != true {
		t.Fatalf("checkin_enabled=%v want true", body["checkin_enabled"])
	}
	_ = time.Now
}

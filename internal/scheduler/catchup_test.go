package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/pool"
	"workbuddy2api/internal/upstream"
)

// catchupStub 统计各任务对应的上游端点调用次数，供当日去重用例断言。
type catchupStub struct {
	checkinCalls   atomic.Int32
	refreshCalls   atomic.Int32
	infoCalls      atomic.Int32
	reportCalls    atomic.Int32
	streakCalls    atomic.Int32
	agreementCalls atomic.Int32
}

func hasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}

func (s *catchupStub) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case hasSuffix(r.URL.Path, "/daily-checkin"):
			s.checkinCalls.Add(1)
			w.Write([]byte(`{"code":0,"msg":"ok","data":{}}`))
		case hasSuffix(r.URL.Path, "/get-user-resource"):
			w.Write([]byte(`{"code":0,"data":{"Response":{"Data":{"Accounts":[{"CycleCapacitySize":100,"CycleCapacityRemain":500,"CycleCapacityUsed":0}]}}}}`))
		case hasSuffix(r.URL.Path, "/token/refresh"):
			s.refreshCalls.Add(1)
			w.Write([]byte(`{"code":0,"data":{"accessToken":"new","expiresIn":3600}}`))
		case hasSuffix(r.URL.Path, "/buddy/info"):
			s.infoCalls.Add(1)
			w.Write([]byte(`{"code":0,"msg":"ok","data":{"buddy":null}}`))
		case hasSuffix(r.URL.Path, "/buddy/agreement"):
			s.agreementCalls.Add(1)
			w.Write([]byte(`{"code":0,"msg":"ok","data":{"agreed":true}}`))
		case hasSuffix(r.URL.Path, "/buddy/first"):
			w.Write([]byte(`{"code":0,"msg":"ok","data":{"buddy":{"id":1,"name":"喵"}}}`))
		case hasSuffix(r.URL.Path, "/streak"):
			s.streakCalls.Add(1)
			w.Write([]byte(`{"code":0,"msg":"ok","data":{"streak":{"days":1}}}`))
		case r.URL.Path == "/v2/report":
			s.reportCalls.Add(1)
			w.Write([]byte(`{"code":0,"msg":"ok","data":{}}`))
		default:
			http.Error(w, "not found", 404)
		}
	})
}

// newCatchupScheduler 构造带运行状态文件的调度器；mutate 可按需覆盖配置。
func newCatchupScheduler(t *testing.T, srv *httptest.Server, statePath string, mutate func(*Config)) (*Scheduler, *pool.Pool) {
	t.Helper()
	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	cfg := Config{
		Pool:         p,
		Upstream:     &upstream.Client{HTTP: srv.Client(), ChatBaseCN: srv.URL, BillingBaseCN: srv.URL},
		RunStatePath: statePath,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return New(cfg), p
}

// TestCatchUpRunsAllEnabledTasks 首次启动（当天未跑）→ 所有启用任务各跑一次并落盘。
// 方案乙核心：不看到没到整点，开机即补。
func TestCatchUpRunsAllEnabledTasks(t *testing.T) {
	fastActivity(t)
	fastTravel(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")
	s, _ := newCatchupScheduler(t, srv, statePath, nil)
	s.CatchUp()

	if n := stub.checkinCalls.Load(); n != 1 {
		t.Errorf("checkin=%d want 1", n)
	}
	if n := stub.reportCalls.Load(); n != 1 {
		t.Errorf("activity report=%d want 1", n)
	}
	if n := stub.infoCalls.Load(); n == 0 {
		t.Errorf("travel buddy/info=%d want >0", n)
	}
	if n := stub.refreshCalls.Load(); n == 0 {
		t.Errorf("keepalive refresh=%d want >0", n)
	}

	// 状态必须落盘（记录的是 CST 自然日），否则重启会重复补跑。
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("state file not written: %v", err)
	}
	var days map[string]string
	if err := json.Unmarshal(raw, &days); err != nil {
		t.Fatalf("state json: %v", err)
	}
	today := travelDay(time.Now())
	for _, key := range []string{"checkin", "travel", "activity", "keepalive"} {
		if days[key] != today {
			t.Errorf("state[%s]=%q want %q", key, days[key], today)
		}
	}
}

// TestCatchUpSecondCallSkipped 同一天内第二次 CatchUp 不再打上游。
func TestCatchUpSecondCallSkipped(t *testing.T) {
	fastActivity(t)
	fastTravel(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")
	s, _ := newCatchupScheduler(t, srv, statePath, nil)

	s.CatchUp()
	first := stub.checkinCalls.Load()
	if first != 1 {
		t.Fatalf("first catch-up checkin=%d want 1", first)
	}

	s.CatchUp() // 同日再来一次
	if n := stub.checkinCalls.Load(); n != 1 {
		t.Fatalf("second catch-up checkin=%d want 1（同日不重复）", n)
	}
}

// TestCatchUpPersistedAcrossRestart 重启（新进程读同一状态文件）后当天不再补跑。
func TestCatchUpPersistedAcrossRestart(t *testing.T) {
	fastActivity(t)
	fastTravel(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")

	s1, _ := newCatchupScheduler(t, srv, statePath, nil)
	s1.CatchUp()
	if n := stub.checkinCalls.Load(); n != 1 {
		t.Fatalf("first process checkin=%d want 1", n)
	}

	// 模拟重启：新调度器读同一文件。
	s2, _ := newCatchupScheduler(t, srv, statePath, nil)
	s2.CatchUp()
	if n := stub.checkinCalls.Load(); n != 1 {
		t.Fatalf("after restart checkin=%d want 1（当天已跑，不补）", n)
	}
}

// TestCatchUpNextDayRuns 跨到次日（CST）后应重新补跑。
func TestCatchUpNextDayRuns(t *testing.T) {
	fastActivity(t)
	fastTravel(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")
	// 预置为"昨天" → 今天应视为未跑。
	yesterday := travelDay(time.Now().Add(-24 * time.Hour))
	if err := writeRunState(statePath, map[string]string{"checkin": yesterday}); err != nil {
		t.Fatal(err)
	}

	s, _ := newCatchupScheduler(t, srv, statePath, func(c *Config) {
		c.TravelDisabled = true
		c.ActivityDisabled = true
		c.KeepaliveDisabled = true
	})
	s.CatchUp()

	if n := stub.checkinCalls.Load(); n != 1 {
		t.Fatalf("checkin=%d want 1（昨天跑过，今天要补）", n)
	}
}

// TestCatchUpDisabled 显式关闭补跑 → 一次上游都不打。
func TestCatchUpDisabled(t *testing.T) {
	fastActivity(t)
	fastTravel(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")
	s, _ := newCatchupScheduler(t, srv, statePath, func(c *Config) {
		c.CatchUpDisabled = true
	})
	s.CatchUp()

	if n := stub.checkinCalls.Load() + stub.reportCalls.Load() + stub.infoCalls.Load(); n != 0 {
		t.Fatalf("upstream calls=%d want 0（补跑已关闭）", n)
	}
}

// TestCatchUpSkipsDisabledTasks 禁用任务不补跑，也不写状态。
func TestCatchUpSkipsDisabledTasks(t *testing.T) {
	fastActivity(t)
	fastTravel(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")
	s, _ := newCatchupScheduler(t, srv, statePath, func(c *Config) {
		c.CheckinDisabled = true
		c.TravelDisabled = true
		c.ActivityDisabled = true
		c.KeepaliveDisabled = true
	})
	s.CatchUp()

	if n := stub.checkinCalls.Load() + stub.reportCalls.Load() + stub.infoCalls.Load(); n != 0 {
		t.Fatalf("upstream calls=%d want 0（四类全禁用）", n)
	}
	if _, err := os.Stat(statePath); err == nil {
		t.Fatalf("state file should not be written when nothing runs")
	}
}

// TestRunStateCorruptFileFailsSoft 状态文件损坏 → 当作没跑过（不阻断启动）。
func TestRunStateCorruptFileFailsSoft(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schedule_state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	rs := newRunState(path)
	if rs.ranOn(taskCheckin, "2026-09-13") {
		t.Fatalf("corrupt file should mean not-ran")
	}
	if err := rs.markDay(taskCheckin, "2026-09-13"); err != nil {
		t.Fatalf("markDay: %v", err)
	}
	if !rs.ranOn(taskCheckin, "2026-09-13") {
		t.Fatalf("after markDay should be ran")
	}
}

// TestRunStateDirCreated 状态文件父目录不存在时自动创建（data/ 首次运行）。
func TestRunStateDirCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "deep", "schedule_state.json")
	rs := newRunState(path)
	if err := rs.markDay(taskCheckin, "2026-09-13"); err != nil {
		t.Fatalf("markDay: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file not created: %v", err)
	}
}

// TestTriggerCheckinDedupesWithinDay 手动触发与定时/补跑共用判定：当天只跑一次。
func TestTriggerCheckinDedupesWithinDay(t *testing.T) {
	fastActivity(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")
	s, _ := newCatchupScheduler(t, srv, statePath, func(c *Config) {
		c.TravelDisabled = true
		c.ActivityDisabled = true
		c.KeepaliveDisabled = true
	})

	if ran := s.TriggerCheckin(); !ran {
		t.Fatal("first trigger should run")
	}
	if ran := s.TriggerCheckin(); ran {
		t.Fatal("second trigger should be deduped")
	}
	if n := stub.checkinCalls.Load(); n != 1 {
		t.Fatalf("checkin calls=%d want 1（第二次被去重）", n)
	}
}

// TestCatchUpThenTimerSkips 启动补跑后，定时到点不再重复执行（三入口共用判定）。
func TestCatchUpThenTimerSkips(t *testing.T) {
	fastActivity(t)
	fastTravel(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")
	s, _ := newCatchupScheduler(t, srv, statePath, func(c *Config) {
		c.TravelDisabled = true
		c.ActivityDisabled = true
		c.KeepaliveDisabled = true
	})

	s.CatchUp()
	if n := stub.checkinCalls.Load(); n != 1 {
		t.Fatalf("catch-up checkin=%d want 1", n)
	}
	// 模拟定时器到点路径。
	if ran := s.runIfNotToday(taskCheckin); ran {
		t.Fatal("timer path should be deduped after catch-up")
	}
	if n := stub.checkinCalls.Load(); n != 1 {
		t.Fatalf("checkin calls=%d want 1（定时到点不重复）", n)
	}
}

// TestRunCatchUpOnStart Run 启动时先补跑当天未跑的任务。
func TestRunCatchUpOnStart(t *testing.T) {
	fastActivity(t)
	fastTravel(t)
	stub := &catchupStub{}
	srv := httptest.NewServer(stub.handler())
	defer srv.Close()

	statePath := filepath.Join(t.TempDir(), "schedule_state.json")
	s, _ := newCatchupScheduler(t, srv, statePath, func(c *Config) {
		c.TravelDisabled = true
		c.ActivityDisabled = true
		c.KeepaliveDisabled = true
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	deadline := time.After(2 * time.Second)
	for stub.checkinCalls.Load() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("Run 未在启动时补跑签到")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未在 ctx 取消后返回")
	}
	if !s.state.ranOn(taskCheckin, travelDay(time.Now())) {
		t.Fatal("startup catch-up should record today")
	}
}

// TestHoursFor 各任务正确取到 hours 与禁用标志。
func TestHoursFor(t *testing.T) {
	s := New(Config{
		CheckinHours:      []int{9, 21},
		TravelDisabled:    true,
		ActivityHours:     []int{10},
		KeepaliveDisabled: false,
	})
	cases := []struct {
		k        taskKind
		disabled bool
	}{
		{taskCheckin, false},
		{taskTravel, true},
		{taskActivity, false},
		{taskKeepalive, false},
	}
	for _, c := range cases {
		if _, dis := s.hoursFor(c.k); dis != c.disabled {
			t.Errorf("%s disabled=%v want %v", taskKey(c.k), dis, c.disabled)
		}
	}
}

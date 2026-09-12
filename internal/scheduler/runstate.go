// runstate.go 定时任务"当天(CST)是否已跑"记录，供开机补跑 / 定时到点 / 手动触发三入口共用去重。
//
// 语义（方案乙）：每个任务按 UTC+8 自然日至多执行一次。
//   - 启动时：当天该任务还没跑过 → 立即执行一次（不看整点是否已到）
//   - 定时到点：当天已跑过 → 跳过；没跑过 → 执行
//   - 手动触发：同上，与定时/补跑共用同一份判定
//
// 因此"开机就优先跑一次"是默认行为，定时器只是当天补漏的兜底。
//
// 落盘 data/schedule_state.json：只记每个任务**最近执行的自然日**（CST，"2006-01-02"）。
// 文件缺失/损坏一律按"没跑过"处理（fail-soft：宁可多跑一次幂等任务，也不静默漏跑）。
package scheduler

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// runState 任务执行日记录。并发安全（定时循环与手动入口可能同时触发）。
type runState struct {
	mu   sync.Mutex
	path string
	days map[string]string // 任务键名 → 最近执行的自然日（CST，"2006-01-02"）
}

// taskKey 任务键名（落盘用可读名而非 iota 数字，便于人工查看 state 文件）。
func taskKey(k taskKind) string {
	switch k {
	case taskCheckin:
		return "checkin"
	case taskTravel:
		return "travel"
	case taskActivity:
		return "activity"
	case taskKeepalive:
		return "keepalive"
	}
	return "unknown"
}

// newRunState 打开（或初始化）状态文件；不存在/损坏按空状态处理，不阻断启动。
func newRunState(path string) *runState {
	rs := &runState{path: path, days: map[string]string{}}
	rs.load()
	return rs
}

// load 读取已有记录；任何失败都降级为空状态（fail-soft，不阻断启动）。
func (r *runState) load() {
	if r.path == "" {
		return
	}
	raw, err := os.ReadFile(r.path)
	if err != nil {
		// 首次运行无文件属正常，不打日志避免噪音；其他错误（权限等）需可见。
		if !os.IsNotExist(err) {
			log.Printf("WARN: [scheduler] 读 %s 失败（按未跑过处理）: %v", r.path, err)
		}
		return
	}
	var days map[string]string
	if err := json.Unmarshal(raw, &days); err != nil {
		log.Printf("WARN: [scheduler] 解析 %s 失败（按未跑过处理）: %v", r.path, err)
		return
	}
	if days != nil {
		r.days = days
	}
}

// ranOn 报告该任务在 day（CST 自然日）是否已执行过。
func (r *runState) ranOn(k taskKind, day string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.days[taskKey(k)] == day
}

// markDay 记录该任务已于 day 执行，并立即落盘（崩溃后仍能避免当日重复执行）。
// 落盘失败返回错误但不回滚内存记录：本次运行已发生，日志提醒即可。
func (r *runState) markDay(k taskKind, day string) error {
	r.mu.Lock()
	r.days[taskKey(k)] = day
	snapshot := make(map[string]string, len(r.days))
	for key, v := range r.days {
		snapshot[key] = v
	}
	r.mu.Unlock()
	return writeRunState(r.path, snapshot)
}

// writeRunState 原子写状态文件（tmp + rename），并确保父目录存在。
// 与 internal/auth.SaveAtomic、internal/pool.saveLocked 同风格：tmp 后缀避免写坏原文件。
func writeRunState(path string, days map[string]string) error {
	if path == "" {
		return nil
	}
	raw, err := json.MarshalIndent(days, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

package statictics

import (
	"fmt"
	"sync"
	"time"
)

type StaticticsUnit struct {
	lasttime   int64
	cum_count  uint64
	cum_time   int64
	last_count uint64
}

func NewDefaultStaticticsUnit() *StaticticsUnit {
	return &StaticticsUnit{
		lasttime:   time.Now().UnixNano(),
		cum_time:   0,
		cum_count:  0,
		last_count: 0,
	}
}
func (stat *StaticticsUnit) Add(count uint64) {
	stat.cum_count += count
}
func (stat StaticticsUnit) ShowTitle() {
	fmt.Printf("%50s %15s %25s %20s %15s %25s\n", "name", "cum", "cum_qps", "interval", "last", "last_qps")
}
func (stat *StaticticsUnit) ShowLine(name string) {
	nowtime := time.Now().UnixNano()

	interval_time := nowtime - stat.lasttime
	interval_count := stat.cum_count - stat.last_count
	interval_qps := float64(interval_count) / float64(interval_time) * 1000000000.0

	stat.lasttime = nowtime
	stat.cum_time += interval_time
	stat.last_count = stat.cum_count

	cum_qps := float64(stat.cum_count) / float64(stat.cum_time) * 1000000000.0

	fmt.Printf("%50s %15d %25.5f %20d %15d %25.5f\n", name, stat.cum_count, cum_qps, interval_time, interval_count, interval_qps)
}

type StaticticsManager struct {
	stat_map map[string]*StaticticsUnit
	mutex    sync.Mutex
}

func NewDefaultStaticticsManager() *StaticticsManager {
	return &StaticticsManager{
		stat_map: make(map[string]*StaticticsUnit),
	}
}
func (manager *StaticticsManager) register_statictics(name string) *StaticticsUnit {
	stat := NewDefaultStaticticsUnit()
	manager.mutex.Lock()
	manager.stat_map[name] = stat
	manager.mutex.Unlock()
	return stat
}
func (manager *StaticticsManager) show() {
	if len(manager.stat_map) > 0 {
		var title bool = false
		for name := range manager.stat_map {
			if !title {
				manager.stat_map[name].ShowTitle()
				title = true
			}
			manager.stat_map[name].ShowLine(name)
		}
	}
}

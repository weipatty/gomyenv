package gomyenv

import (
	"fmt"
	"github.com/rcrowley/go-metrics"
	"os"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	exit := GetExitChan()

	fmt.Println("begin")
	c := metrics.NewCounter()
	metrics.Register("foo", c)
	c.Inc(47)
	s := metrics.NewExpDecaySample(1028, 0.015) // or metrics.NewUniformSample(1028)
	h := metrics.NewHistogram(s)
	metrics.Register("baz", h)
	h.Update(47)
	metrics.Each(func(s string, i interface{}) {
		fmt.Println("d", s, i)
	})

	p := "/root/tmp"
	go RunReportPath(metrics.DefaultRegistry, 1*time.Second, p, exit)
	var cnt int64 = 1
	for {
		select {
		case <-exit:
			fmt.Println("main exit")
			os.Exit(0)
		case <-time.After(time.Second):
			fmt.Println("sleep")
			c.Inc(47)
			h.Update(cnt)
			h.Update(cnt)
			h.Update(cnt)
			h.Update(cnt)
			cnt += 1
		}

	}

}

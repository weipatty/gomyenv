package gomyenv

import (
	"encoding/json"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/rcrowley/go-metrics"
	"io"
	"time"
)

type MetircsRow struct {
	MetricsName string `json:"metricsName"`
	Timestamp   int64  `json:"timestamp"`
	LongValue   int64  `json:"longValue"`
}

func writeMetrics(l io.Writer, r metrics.Registry) {

	row := MetircsRow{
		Timestamp: time.Now().UnixNano() / 1000,
	}

	r.Each(func(name string, i interface{}) {

		row.MetricsName = name

		switch metric := i.(type) {
		case metrics.Counter:
			row.LongValue = metric.Count()
		case metrics.Histogram:
			row.LongValue = int64(metric.Snapshot().Mean())
		case metrics.Meter:
			row.LongValue = int64(metric.Snapshot().RateMean())
		}
		buf, err := json.Marshal(row)
		if err == nil {
			l.Write(buf)
			l.Write([]byte("\n"))
		}
	})

}

func RunReportPath(r metrics.Registry, freq time.Duration, p string, exit chan struct{}) {
	//sc := make(chan os.Signal, 1)
	//signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	logf, err := rotatelogs.New(
		p+".%Y%m%d%H",
		rotatelogs.WithLinkName(p),
		rotatelogs.WithMaxAge(90*24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour),
		rotatelogs.ForceNewFile(),
	)
	if err != nil {
		return
	}
	defer logf.Close()
Loop:
	for {
		select {
		case <-exit:
			//logf.Write([]byte("exit"))
			break Loop
		case <-time.After(freq):
			writeMetrics(logf, r)
		}

	}
}

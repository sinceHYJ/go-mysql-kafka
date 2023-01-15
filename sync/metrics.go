package sync

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	canalDelay = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "mysql2Kafka_canal_delay",
			Help: "The canal slave lag",
		},
	)
)

func (c *CanalService) collectMetrics() {
	for range time.Tick(10 * time.Second) {
		canalDelay.Set(float64(c.canal.GetDelay()))
	}
}
func InitStatus(addr string, path string) {
	http.Handle(path, promhttp.Handler())
	_ = http.ListenAndServe(addr, nil)
}

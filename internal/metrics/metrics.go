package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	TotalMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "total_messages",
			Help: "Total number of messages processed",
		}, []string{"round", "protocol", "node_id", "sid"},
	)

	ValidMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "valid_messages",
			Help: "Number of valid messages processed",
		}, []string{"round", "protocol", "node_id", "sid"},
	)
)

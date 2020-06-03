package zetta

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/alexcesaro/statsd.v2"
)

//For quick debug, change to prometheus later.
var (
	unit   = os.Getenv("ZAE_UNIT_NAME")
	cli    *statsd.Client
	prefix = fmt.Sprintf("zetta.client.%s", unit)
)

func init() {
	var err error
	cli, err = statsd.New(statsd.Address("status:8126"))
	if err != nil {
		log.Println("Error: statsd init:", err)
	}
}

func metricCount(op string) {
	m := fmt.Sprintf("%s.%s.count", prefix, op)
	cli.Increment(m)
}

func metricCountError(op string) {
	m := fmt.Sprintf("%s.%s.count.err", prefix, op)
	cli.Increment(m)
}

func metricStartTiming() statsd.Timing {
	return cli.NewTiming()
}

func metricRecordTiming(t statsd.Timing, op string) {
	m := fmt.Sprintf("%s.%s", prefix, op)
	cli.Timing(m, t.Duration().Seconds()*1000)
}

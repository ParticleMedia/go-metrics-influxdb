package influxdb

import (
	"fmt"
	"log"
	uurl "net/url"
	"strings"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client"
	"github.com/rcrowley/go-metrics"
)

type Reporter struct {
	reg      metrics.Registry
	interval time.Duration
	align    bool
	url      uurl.URL
	database string

	measurement string
	username    string
	password    string
	tags        map[string]string

	client *client.Client

	done chan struct{}
	wg   *sync.WaitGroup
}

// InfluxDB starts a InfluxDB reporter which will post the metrics from the given registry at each d interval.
func InfluxDB(r metrics.Registry, d time.Duration, url, database, measurement, username, password string, align bool) *Reporter {
	return InfluxDBWithTags(r, d, url, database, measurement, username, password, map[string]string{}, align)
}

// InfluxDBWithTags starts a InfluxDB reporter which will post the metrics from the given registry at each d interval with the specified tags
func InfluxDBWithTags(r metrics.Registry, d time.Duration, url, database, measurement, username, password string, tags map[string]string, align bool) *Reporter {
	u, err := uurl.Parse(url)
	if err != nil {
		log.Printf("unable to parse InfluxDB url %s. err=%v", url, err)
		return nil
	}

	rep := &Reporter{
		reg:         r,
		interval:    d,
		url:         *u,
		database:    database,
		measurement: measurement,
		username:    username,
		password:    password,
		tags:        tags,
		align:       align,
		done:        make(chan struct{}),
		wg:          &sync.WaitGroup{},
	}
	if err := rep.makeClient(); err != nil {
		log.Printf("unable to make InfluxDB client. err=%v", err)
		return nil
	}

	go rep.run()
	return rep
}

func (r *Reporter) makeClient() (err error) {
	r.client, err = client.NewClient(client.Config{
		URL:      r.url,
		Username: r.username,
		Password: r.password,
	})

	return
}

func (r *Reporter) Close() {
	close(r.done)
	r.wg.Wait()
}

func (r *Reporter) run() {
	r.wg.Add(1)
	defer r.wg.Done()

	intervalTicker := time.Tick(r.interval)
	pingTicker := time.Tick(time.Second * 5)

	for {
		select {
		case <-intervalTicker:
			if err := r.send(); err != nil {
				log.Printf("unable to send metrics to InfluxDB. err=%v", err)
			}
		case <-pingTicker:
			_, _, err := r.client.Ping()
			if err != nil {
				log.Printf("got error while sending a ping to InfluxDB, trying to recreate client. err=%v", err)

				if err = r.makeClient(); err != nil {
					log.Printf("unable to make InfluxDB client. err=%v", err)
				}
			}
		case <-r.done:
			if err := r.send(); err != nil {
				log.Printf("unable to send metrics to InfluxDB. err=%v", err)
			}
			return
		}
	}
}

func (r *Reporter) send() error {
	var pts []client.Point

	now := time.Now()
	if r.align {
		now = now.Truncate(r.interval)
	}
	r.reg.Each(func(name string, i interface{}) {
		tags := r.tags
		if idx := strings.IndexRune(name, ','); idx != -1 {
			tags := map[string]string{}
			for k, v := range r.tags {
				tags[k] = v
			}
			for _, kv := range strings.Split(name[idx+1:], ",") {
				if tagIdx := strings.IndexRune(kv, '='); tagIdx != -1 {
					tags[kv[:tagIdx]] = kv[tagIdx+1:]
				}
			}
			name = name[:idx]
		}

		switch metric := i.(type) {
		case metrics.Counter:
			ms := metric.Snapshot()
			pts = append(pts, client.Point{
				Measurement: r.measurement,
				Tags:        tags,
				Fields: map[string]interface{}{
					fmt.Sprintf("%s.count", name): ms.Count(),
				},
				Time: now,
			})
		case metrics.Gauge:
			ms := metric.Snapshot()
			pts = append(pts, client.Point{
				Measurement: r.measurement,
				Tags:        tags,
				Fields: map[string]interface{}{
					fmt.Sprintf("%s.gauge", name): ms.Value(),
				},
				Time: now,
			})
		case metrics.GaugeFloat64:
			ms := metric.Snapshot()
			pts = append(pts, client.Point{
				Measurement: r.measurement,
				Tags:        tags,
				Fields: map[string]interface{}{
					fmt.Sprintf("%s.gauge", name): ms.Value(),
				},
				Time: now,
			})
		case metrics.Histogram:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fields := map[string]float64{
				"count":    float64(ms.Count()),
				"max":      float64(ms.Max()),
				"mean":     ms.Mean(),
				"min":      float64(ms.Min()),
				"stddev":   ms.StdDev(),
				"variance": ms.Variance(),
				"p50":      ps[0],
				"p75":      ps[1],
				"p95":      ps[2],
				"p99":      ps[3],
				"p999":     ps[4],
				"p9999":    ps[5],
			}
			for k, v := range fields {
				pts = append(pts, client.Point{
					Measurement: r.measurement,
					Tags:        bucketTags(k, tags),
					Fields: map[string]interface{}{
						fmt.Sprintf("%s.histogram", name): v,
					},
					Time: now,
				})

			}
		case metrics.Meter:
			ms := metric.Snapshot()
			fields := map[string]float64{
				"count": float64(ms.Count()),
				"m1":    ms.Rate1(),
				"m5":    ms.Rate5(),
				"m15":   ms.Rate15(),
				"mean":  ms.RateMean(),
			}
			for k, v := range fields {
				pts = append(pts, client.Point{
					Measurement: r.measurement,
					Tags:        bucketTags(k, tags),
					Fields: map[string]interface{}{
						fmt.Sprintf("%s.meter", name): v,
					},
					Time: now,
				})
			}

		case metrics.Timer:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fields := map[string]float64{
				"count":    float64(ms.Count()),
				"max":      float64(ms.Max()),
				"mean":     ms.Mean(),
				"min":      float64(ms.Min()),
				"stddev":   ms.StdDev(),
				"variance": ms.Variance(),
				"p50":      ps[0],
				"p75":      ps[1],
				"p95":      ps[2],
				"p99":      ps[3],
				"p999":     ps[4],
				"p9999":    ps[5],
				"m1":       ms.Rate1(),
				"m5":       ms.Rate5(),
				"m15":      ms.Rate15(),
				"meanrate": ms.RateMean(),
			}
			for k, v := range fields {
				pts = append(pts, client.Point{
					Measurement: r.measurement,
					Tags:        bucketTags(k, tags),
					Fields: map[string]interface{}{
						fmt.Sprintf("%s.timer", name): v,
					},
					Time: now,
				})
			}
		}
	})

	bps := client.BatchPoints{
		Points:   pts,
		Database: r.database,
	}

	_, err := r.client.Write(bps)
	return err
}

func bucketTags(bucket string, tags map[string]string) map[string]string {
	m := map[string]string{}
	for tk, tv := range tags {
		m[tk] = tv
	}
	m["bucket"] = bucket
	return m
}

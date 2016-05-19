package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/therealbill/libredis/client"
	"github.com/therealbill/libredis/structures"
)

// RedisHost represents a set of Redis Hosts to health check.
type RedisHost struct {
	Addrs     []string
	Passwords []string
}

// Exporter implementes the prometheus.Exporter interface, and exports Redis metrics.
type Exporter struct {
	redis        RedisHost
	namespace    string
	duration     prometheus.Gauge
	scrapeErrors prometheus.Gauge
	totalScrapes prometheus.Counter
	metrics      map[string]*prometheus.GaugeVec
	sync.RWMutex
}

type scrapeResult struct {
	Name  string
	Value interface{}
	Addr  string
	DB    string
}

var commandEntries []structures.CommandEntry
var commandMap map[string]structures.CommandEntry

func init() {
	commandMap = make(map[string]structures.CommandEntry)
}

func (e *Exporter) initGauges() {

	e.metrics = map[string]*prometheus.GaugeVec{}
	e.metrics["db_keys_total"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "db_keys_total",
		Help:      "Total number of keys by DB",
	}, []string{"addr", "db"})
	e.metrics["db_expiring_keys_total"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "db_expiring_keys_total",
		Help:      "Total number of expiring keys by DB",
	}, []string{"addr", "db"})

	e.metrics["db_avg_ttl_seconds"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "db_avg_ttl_seconds",
		Help:      "Avg TTL in seconds",
	}, []string{"addr", "db"})

	e.metrics["uptime_in_seconds"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "uptime_in_seconds",
		Help:      "Time instance has been up, measured in seconds",
	}, []string{"addr"})

	e.metrics["used_memory"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "used_memory",
		Help:      "Memory used by data and buffers, in bytes",
	}, []string{"addr"})

	e.metrics["used_memory_peak"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "used_memory_peak",
		Help:      "Peak used by data and buffers, in bytes",
	}, []string{"addr"})

	e.metrics["used_memory_rss"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "used_memory_rss",
		Help:      "Resident Set Size by the Redis instance, in bytes",
	}, []string{"addr"})

	e.metrics["used_memory_lua"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "used_memory_lua",
		Help:      "Memory used by the Lua script system, in bytes",
	}, []string{"addr"})

	e.metrics["total_system_memory"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "total_system_memory",
		Help:      "Total memory available to the system, in bytes",
	}, []string{"addr"})

	e.metrics["used_cpu_sys"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "used_cpu_sys",
		Help:      "System CPU Used by Redis",
	}, []string{"addr"})

	e.metrics["used_cpu_user"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "used_cpu_sys",
		Help:      "User CPU Used by Redis",
	}, []string{"addr"})

	e.metrics["used_cpu_user_children"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "used_cpu_user_children",
		Help:      "User CPU Used by Redis child processes",
	}, []string{"addr"})

	e.metrics["used_cpu_sys_children"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "used_cpu_sys_children",
		Help:      "System CPU Used by Redis child processes",
	}, []string{"addr"})

	e.metrics["mem_fragmentation_ratio"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "mem_fragmentation_ratio",
		Help:      "Memory fragmentation ratio",
	}, []string{"addr"})
}

// NewRedisExporter returns a new exporter of Redis metrics.
func NewRedisExporter(redis RedisHost, namespace string) *Exporter {
	e := Exporter{
		redis:     redis,
		namespace: namespace,

		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_last_scrape_duration_seconds",
			Help:      "The last scrape duration.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total redis scrapes.",
		}),
		scrapeErrors: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_last_scrape_error",
			Help:      "The last scrape error status.",
		}),
	}
	log.Printf("exporter for %+v", redis)
	e.initGauges()
	return &e
}

// Describe outputs Redis metric descriptions.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {

	for _, m := range e.metrics {
		m.Describe(ch)
	}
	ch <- e.duration.Desc()
	ch <- e.totalScrapes.Desc()
	ch <- e.scrapeErrors.Desc()
}

// Collect fetches new metrics from the RedisHost and updates the appropriate metrics.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	scrapes := make(chan scrapeResult)

	e.Lock()
	defer e.Unlock()

	e.initGauges()
	go e.scrape(scrapes)
	e.setMetrics(scrapes)

	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.scrapeErrors
	e.collectMetrics(ch)
}

func extractInfoMetrics(info structures.RedisInfoAll, addr string, scrapes chan<- scrapeResult) error {
	db := ""
	log.Print("Converting Redis Info metrics to scrapeResults")
	//scrapes <- scrapeResult{Name: "NAME", Addr: addr, DB: db, Value: info.SEC.KEY}

	// Server
	scrapes <- scrapeResult{Name: "uptime_in_seconds", Addr: addr, DB: db, Value: info.Server.UptimeInSeconds}

	// CPU
	scrapes <- scrapeResult{Name: "used_cpu_sys", Addr: addr, DB: db, Value: info.CPU.UsedCPUSystem}
	scrapes <- scrapeResult{Name: "used_cpu_user", Addr: addr, DB: db, Value: info.CPU.UsedCPUUser}
	scrapes <- scrapeResult{Name: "used_cpu_sys_children", Addr: addr, DB: db, Value: info.CPU.UsedCPUChildren}
	scrapes <- scrapeResult{Name: "used_cpu_user_children", Addr: addr, DB: db, Value: info.CPU.UsedCPUUserChildren}
	// Memory
	scrapes <- scrapeResult{Name: "used_memory", Addr: addr, DB: db, Value: info.Memory.UsedMemory}
	scrapes <- scrapeResult{Name: "used_memory_peak", Addr: addr, DB: db, Value: info.Memory.UsedMemoryPeak}
	scrapes <- scrapeResult{Name: "used_memory_rss", Addr: addr, DB: db, Value: info.Memory.UsedMemoryRss}
	scrapes <- scrapeResult{Name: "used_memory_lua", Addr: addr, DB: db, Value: info.Memory.UsedMemoryLua}
	scrapes <- scrapeResult{Name: "mem_fragmentation_ratio", Addr: addr, DB: db, Value: info.Memory.MemoryFragmentationRatio}
	scrapes <- scrapeResult{Name: "total_system_memory", Addr: addr, DB: db, Value: info.Memory.TotalSystemMemory}

	// Clients
	scrapes <- scrapeResult{Name: "connected_clients", Addr: addr, DB: db, Value: info.Client.ConnectedClients}
	scrapes <- scrapeResult{Name: "client_longest_output_list", Addr: addr, DB: db, Value: info.Client.ClientLongestOutputList}
	scrapes <- scrapeResult{Name: "client_biggest_input_buf", Addr: addr, DB: db, Value: info.Client.ClientBiggestInputBuffer}
	scrapes <- scrapeResult{Name: "blocked_clients", Addr: addr, DB: db, Value: info.Client.BlockedClients}

	// Persistence
	scrapes <- scrapeResult{Name: "changes_since_save", Addr: addr, DB: db, Value: info.Persistence.ChangesSinceSave}
	scrapes <- scrapeResult{Name: "aof_last_rewrite_time_sec", Addr: addr, DB: db, Value: info.Persistence.LastRewriteTimeInSeconds}

	// Stats
	scrapes <- scrapeResult{Name: "total_connections_received", Addr: addr, DB: db, Value: info.Stats.TotalConnectionsReceived}
	scrapes <- scrapeResult{Name: "total_commands_processed", Addr: addr, DB: db, Value: info.Stats.TotalCommandsProcessed}
	scrapes <- scrapeResult{Name: "instantaneous_ops_per_sec", Addr: addr, DB: db, Value: info.Stats.InstanteousOpsPerSecond}
	scrapes <- scrapeResult{Name: "total_net_input_bytes", Addr: addr, DB: db, Value: info.Stats.TotalNetInputBytes}
	scrapes <- scrapeResult{Name: "total_net_output_bytes", Addr: addr, DB: db, Value: info.Stats.TotalNetOutputBytes}
	scrapes <- scrapeResult{Name: "rejected_connections", Addr: addr, DB: db, Value: info.Stats.RejectedConnections}
	scrapes <- scrapeResult{Name: "sync_full", Addr: addr, DB: db, Value: info.Stats.SyncFull}
	scrapes <- scrapeResult{Name: "sync_partial_ok", Addr: addr, DB: db, Value: info.Stats.SyncPartialOk}
	scrapes <- scrapeResult{Name: "sync_partial_err", Addr: addr, DB: db, Value: info.Stats.SyncPartialErr}
	scrapes <- scrapeResult{Name: "expired_keys", Addr: addr, DB: db, Value: info.Stats.ExpiredKeys}
	scrapes <- scrapeResult{Name: "evicted_keys", Addr: addr, DB: db, Value: info.Stats.EvictedKeys}
	scrapes <- scrapeResult{Name: "keyspace_hits", Addr: addr, DB: db, Value: info.Stats.KeyspaceHits}
	scrapes <- scrapeResult{Name: "keyspace_misses", Addr: addr, DB: db, Value: info.Stats.KeyspaceMisses}
	scrapes <- scrapeResult{Name: "pubsub_channels", Addr: addr, DB: db, Value: info.Stats.PubSubChannels}
	scrapes <- scrapeResult{Name: "pubsub_patterns", Addr: addr, DB: db, Value: info.Stats.PubSubPatterns}
	scrapes <- scrapeResult{Name: "latest_fork_usec", Addr: addr, DB: db, Value: info.Stats.LatestForkUsec}
	scrapes <- scrapeResult{Name: "migrate_cached_sockets", Addr: addr, DB: db, Value: info.Stats.MigrateCachedSockets}

	// Command Stats
	admin_cnt := 0.0
	readonly_cnt := 0.0
	write_cnt := 0.0
	pubsub_cnt := 0.0
	for cmd, stats := range info.Commandstats.Stats {
		for s, v := range stats {
			name := fmt.Sprintf("commandstats_%s_%s", cmd, s)
			ce, exists := commandMap[cmd]
			if exists && s == "calls" {
				if ce.ReadOnly() {
					readonly_cnt += v
				}
				if ce.Admin() {
					admin_cnt += v
				}
				if ce.Pubsub() {
					pubsub_cnt += v
				}
				if ce.Writes() {
					write_cnt += v
				}

			}
			scrapes <- scrapeResult{Name: name, Addr: addr, DB: db, Value: v}
		}
	}
	scrapes <- scrapeResult{Name: "admin_command_calls", Addr: addr, DB: db, Value: admin_cnt}
	scrapes <- scrapeResult{Name: "readonly_command_calls", Addr: addr, DB: db, Value: readonly_cnt}
	scrapes <- scrapeResult{Name: "write_command_calls", Addr: addr, DB: db, Value: write_cnt}
	scrapes <- scrapeResult{Name: "pubsub_command_calls", Addr: addr, DB: db, Value: pubsub_cnt}

	// Keyspace
	for _, space := range info.Keyspace.Databases {
		db := fmt.Sprintf("%d", space["db"])
		scrapes <- scrapeResult{Name: "db_keys_total", Addr: addr, DB: db, Value: space["keys"]}
		scrapes <- scrapeResult{Name: "db_expiring_keys_total", Addr: addr, DB: db, Value: space["expires"]}
		scrapes <- scrapeResult{Name: "db_avg_ttl_seconds", Addr: addr, DB: db, Value: space["avg_ttl"]}
	}

	return nil
}

func extractConfigMetrics(config []string, addr string, scrapes chan<- scrapeResult) error {

	if len(config)%2 != 0 {
		return fmt.Errorf("invalid config: %#v", config)
	}

	for pos := 0; pos < len(config)/2; pos++ {
		val, err := strconv.ParseFloat(config[pos*2+1], 64)
		if err != nil {
			log.Printf("couldn't parse %s, err: %s", config[pos*2+1], err)
			continue
		}
		scrapes <- scrapeResult{Name: fmt.Sprintf("config_%s", config[pos*2]), Addr: addr, Value: val}
	}
	return nil
}

func getCommands(conn *client.Redis) {
	if len(commandEntries) != 0 {
		return
	}
	var err error
	commandEntries, err = conn.Command()
	if err != nil {
		log.Printf("Unable to acquire command entries, metrics degradated")
		return
	}
	for _, cmd := range commandEntries {
		commandMap[cmd.Name] = cmd
	}
}

func (e *Exporter) scrape(scrapes chan<- scrapeResult) {

	defer close(scrapes)
	now := time.Now().UnixNano()
	e.totalScrapes.Inc()

	errorCount := 0
	for idx, addr := range e.redis.Addrs {
		log.Printf("Now connecting to %s", addr)
		dc := client.DialConfig{Address: addr, Network: "tcp"}
		if len(e.redis.Passwords) > idx && e.redis.Passwords[idx] != "" {
			dc.Password = e.redis.Passwords[idx]
		}
		c, err := client.DialWithConfig(&dc)
		if err != nil {
			log.Printf("redis err: %s", err)
			errorCount++
			continue
		}
		getCommands(c)
		log.Printf("Connected to %s", addr)
		info, err := c.Info()
		if err == nil {
			err = extractInfoMetrics(info, addr, scrapes)
		}
		if err != nil {
			log.Printf("redis err: %s", err)
			errorCount++
		}

	}

	e.scrapeErrors.Set(float64(errorCount))
	e.duration.Set(float64(time.Now().UnixNano()-now) / 1000000000)
}

func (e *Exporter) setMetrics(scrapes <-chan scrapeResult) {
	for scr := range scrapes {
		name := scr.Name
		if _, ok := e.metrics[name]; !ok {
			e.metrics[name] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: e.namespace,
				Name:      name,
			}, []string{"addr"})
		}
		var labels prometheus.Labels = map[string]string{"addr": scr.Addr}
		if len(scr.DB) > 0 {
			labels["db"] = scr.DB
		}
		switch scr.Value.(type) {
		case float64:
			e.metrics[name].With(labels).Set(scr.Value.(float64))
		case int64:
			e.metrics[name].With(labels).Set(float64(scr.Value.(int64)))
		case int:
			e.metrics[name].With(labels).Set(float64(scr.Value.(int)))
		}
	}
}

func (e *Exporter) collectMetrics(metrics chan<- prometheus.Metric) {
	for _, m := range e.metrics {
		m.Collect(metrics)
	}
}

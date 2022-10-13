package http

import (
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
Reads events from HTTP requests with the body delimited by a new line.

Also, it emulates some protocols to allow receiving events from a wide range of software that use HEP to transmit data.
E.g. `file.d` may pretend to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HEP code `OK 200` right after it has read all the request body.
> It doesn't wait until events are committed.

**Example:**
Emulating elastic through hep:
```yaml
pipelines:
  example_k8s_pipeline:
    settings:
      capacity: 1024
    input:
      # define input type.
      type: hep
      # pretend elastic search, emulate it's protocol.
      emulate_mode: "elasticsearch"
      # define hep port.
      address: ":9063"
    actions:
      # parse elastic search query.
      - type: parse_es
      # decode elastic search json.
      - type: json_decode
        # field is required.
        field: message
    output:
      # Let's write to kafka example.
      type: kafka
        brokers: [kafka-local:9092, kafka-local:9091]
        default_topic: yourtopic-k8s-data
        use_topic_field: true
        topic_field: pipeline_kafka_topic

      # Or we can write to file:
      # type: file
      # target_file: "./output.txt"
```

Setup:
```
# run server.
# config.yaml should contains yaml config above.
go run cmd/file.d.go --config=config.yaml

# now do requests.
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"index":{"_index":"index-main","_type":"span"}}
{"message": "hello", "kind": "normal"}
'

##

}*/

const (
	subsystemName     = "input_hep"
	httpErrorCounter  = "hep_errors"
	readBufDefaultLen = 16 * 1024
	maxPktLen         = 65507
)

type Plugin struct {
	config     *Config
	params     *pipeline.InputPluginParams
	readBuffs  *sync.Pool
	eventBuffs *sync.Pool
	controller pipeline.InputPluginController
	server     *net.UDPAddr
	receiver   *net.UDPConn
	sourceIDs  []pipeline.SourceID
	sourceSeq  pipeline.SourceID
	mu         *sync.Mutex
	logger     *zap.SugaredLogger
	buffer     *sync.Pool
	stopped    uint32
	stats      HEPStats
	inputCh    chan []byte
}

type HEPStats struct {
	DupCount uint64
	ErrCount uint64
	HEPCount uint64
	PktCount uint64
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > An address to listen to. Omit ip/host to listen all network interfaces. E.g. `:88`
	Address string `json:"address" default:":9063"` // *
	// > @3@4@5@6
	// >
	// > Which protocol to emulate.
	EmulateMode string `json:"emulate_mode" default:"no" options:"no|elasticsearch"` // *
	// > @3@4@5@6
	// >
	// > CA certificate in PEM encoding. This can be a path or the content of the certificate.
	// > If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.
	CACert string `json:"ca_cert" default:""` // *
	// > @3@4@5@6
	// >
	// > CA private key in PEM encoding. This can be a path or the content of the key.
	// > If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.
	PrivateKey string `json:"private_key" default:""` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "hep",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.config = config.(*Config)
	p.params = params
	p.logger = params.Logger
	p.readBuffs = &sync.Pool{}
	p.eventBuffs = &sync.Pool{}
	p.mu = &sync.Mutex{}
	p.controller = params.Controller
	p.controller.DisableStreams()
	p.sourceIDs = make([]pipeline.SourceID, 0)
	var err error

	p.registerPluginMetrics()

	mux := http.NewServeMux()
	switch p.config.EmulateMode {
	case "elasticsearch":
		p.elasticsearch(mux)
	case "no":
		mux.HandleFunc("/", p.serve)
	}

	p.server, err = net.ResolveUDPAddr("udp", "10.0.0.1")
	if err != nil {
		log.Fatalln(err)
	}

	p.receiver, err = net.ListenUDP("udp", p.server)
	if err != nil {
		log.Fatalln(err)
	}

	if p.config.Address != "off" {
		longpanic.Go(p.listenHEP)
	}
}

func (p *Plugin) registerPluginMetrics() {
	metric.RegisterCounter(&metric.MetricDesc{
		Subsystem: subsystemName,
		Name:      httpErrorCounter,
		Help:      "Total http errors",
	})
}

func (p *Plugin) listenHEP() {

	defer func() {
		log.Printf("stopping UDP listener on %s", p.receiver.LocalAddr())
		p.receiver.Close()
	}()

	for {
		if atomic.LoadUint32(&p.stopped) == 1 {
			return
		}
		p.receiver.SetReadDeadline(time.Now().Add(1e9))
		buf := p.buffer.Get().([]byte)
		n, err := p.receiver.Read(buf)
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			} else {
				log.Printf("%v", err)
				return
			}
		} else if n > maxPktLen {
			log.Printf("received too big packet with %d bytes", n)
			atomic.AddUint64(&p.stats.ErrCount, 1)
			continue
		}
		p.inputCh <- buf[:n]
		atomic.AddUint64(&p.stats.PktCount, 1)
	}
}

func (p *Plugin) newReadBuff() []byte {
	if buff := p.readBuffs.Get(); buff != nil {
		return *buff.(*[]byte)
	}
	return make([]byte, readBufDefaultLen)
}

func (p *Plugin) newEventBuffs() []byte {
	if buff := p.eventBuffs.Get(); buff != nil {
		return (*buff.(*[]byte))[:0]
	}
	return make([]byte, 0, p.params.PipelineSettings.AvgEventSize)
}

func (p *Plugin) getSourceID() pipeline.SourceID {
	p.mu.Lock()
	if len(p.sourceIDs) == 0 {
		p.sourceIDs = append(p.sourceIDs, p.sourceSeq)
		p.sourceSeq++
	}

	l := len(p.sourceIDs)
	x := p.sourceIDs[l-1]
	p.sourceIDs = p.sourceIDs[:l-1]
	p.mu.Unlock()

	return x
}

func (p *Plugin) putSourceID(x pipeline.SourceID) {
	p.mu.Lock()
	p.sourceIDs = append(p.sourceIDs, x)
	p.mu.Unlock()
}

func (p *Plugin) serve(w http.ResponseWriter, r *http.Request) {
	readBuff := p.newReadBuff()
	eventBuff := p.newEventBuffs()

	sourceID := p.getSourceID()
	defer p.putSourceID(sourceID)

	for {
		n, err := r.Body.Read(readBuff)
		if n == 0 && err == io.EOF {
			break
		}

		if err != nil && err != io.EOF {
			metric.GetCounter(subsystemName, httpErrorCounter).Inc()
			logger.Errorf("http input read error: %s", err.Error())
			break
		}

		eventBuff = p.processChunk(sourceID, readBuff[:n], eventBuff, false)
	}

	if len(eventBuff) > 0 {
		eventBuff = p.processChunk(sourceID, readBuff[:0], eventBuff, true)
	}

	_ = r.Body.Close()

	// https://staticcheck.io/docs/checks/#SA6002
	p.readBuffs.Put(&readBuff)
	p.eventBuffs.Put(&eventBuff)

	_, err := w.Write(result)
	if err != nil {
		metric.GetCounter(subsystemName, httpErrorCounter).Inc()
		logger.Errorf("can't write response: %s", err.Error())
	}
}

func (p *Plugin) processChunk(sourceID pipeline.SourceID, readBuff []byte, eventBuff []byte, isLastChunk bool) []byte {
	pos := 0   // current position
	nlPos := 0 // new line position
	for pos < len(readBuff) {
		if readBuff[pos] != '\n' {
			pos++
			continue
		}

		if len(eventBuff) != 0 {
			eventBuff = append(eventBuff, readBuff[nlPos:pos]...)
			_ = p.controller.In(sourceID, "hep", int64(pos), eventBuff, true)
			eventBuff = eventBuff[:0]
		} else {
			_ = p.controller.In(sourceID, "hep", int64(pos), readBuff[nlPos:pos], true)
		}

		pos++
		nlPos = pos
	}

	if isLastChunk {
		// flush buffers if we can't find the newline character
		_ = p.controller.In(sourceID, "hep", int64(pos), append(eventBuff, readBuff[nlPos:]...), true)
		eventBuff = eventBuff[:0]
	} else {
		eventBuff = append(eventBuff, readBuff[nlPos:]...)
	}

	return eventBuff
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(_ *pipeline.Event) {
	// todo: don't reply with OK till all events in request will be committed
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(event *pipeline.Event) bool {
	return true
}

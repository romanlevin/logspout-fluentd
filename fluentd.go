package fluentd

import (
	"encoding/json"
	"errors"
	"log"
	"net"

	"github.com/gliderlabs/logspout/router"
)

// FluentdAdapter is an adapter for streaming JSON to a fluentd collector.
type FluentdAdapter struct {
	conn  net.Conn
	route *router.Route
}

// Stream handles a stream of messages from Logspout. Implements router.logAdapter.
func (adapter *FluentdAdapter) Stream(logstream chan *router.Message) {
	for message := range logstream {
		dockerInfo := DockerInfo{
			Name:     message.Container.Name,
			Service:  message.Container.Config.Labels["com.docker.compose.service"],
			ID:       message.Container.ID,
			Image:    message.Container.Config.Image,
			Hostname: message.Container.Config.Hostname,
		}

		var js []byte
		var err error
		var record map[string]interface{}

		// Parse JSON-encoded message.Data
		if err := json.Unmarshal([]byte(message.Data), &record); err != nil {
			// The message is not in JSON, make a new JSON message.
			record = make(map[string]interface{})
			record["message"] = message.Data
			record["docker"] = dockerInfo
			record["stream"] = message.Source

		} else {
			// The message is already in JSON, add the docker specific fields.
			record["docker"] = dockerInfo
			record["stream"] = message.Source
		}

		timestamp := float64(message.Time.UnixNano()) / 1000000000
		tag := "logspout"

		// Fluentd "in_forward" plugin `expects events to be an array object like: [tag, time ,record]
		// or else it threats them as "broken chunks" (not documented in fluentd)
		data := []interface{}{tag, timestamp, record}

		if js, err = json.Marshal(data); err != nil {
			// Log error message and continue parsing next line, if marshalling fails
			log.Println("fluentd-adapter: could not marshal JSON:", err)
			continue
		}

		// To work with tls and tcp transports via json_lines codec
		js = append(js, byte('\n'))

		if _, err := adapter.conn.Write(js); err != nil {
			// There is no retry option implemented yet
			log.Fatal("fluentd-adapter: could not write:", err)
		}
	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	Service  string `json:"service"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

type LogglyMessage struct {
	Message string     `json:"message"`
	Stream  string     `json:"stream"`
	Docker  DockerInfo `json:"docker"`
}

// NewFluentdAdapter creates a Logspout fluentd adapter instance.
func NewFluentdAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("tcp"))

	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &FluentdAdapter{
		conn:  conn,
		route: route,
	}, nil
}

func init() {
	router.AdapterFactories.Register(NewFluentdAdapter, "fluentd-tcp")
}

package client

import (
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// prometheus metrics
// var promSenderCounter = promauto.NewCounterVec(prometheus.CounterOpts{
// 	Name: "alpaca_receiver_sender",
// 	Help: "messages count from senders",
// }, []string{"type"})

type NatsClient struct {
	Conn *nats.Conn
	Js   nats.JetStreamContext
}

// NewNatsClient returns a Connection and Encoded Connection
// both need to defer .Close()
func NewNatsClient(endpoint string) *NatsClient {
	endpoint = "nats://" + endpoint
	nc, err := nats.Connect(endpoint)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("connected to nats endpoint", endpoint)
	log.Println("nats version:", nc.ConnectedServerVersion())

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	return &NatsClient{
		Conn: nc,
		Js:   js,
	}
}

func NewNatsHandler[T any](ch chan T) nats.MsgHandler {
	return func(msg *nats.Msg) {
		var thing T
		if err := json.Unmarshal(msg.Data, &thing); err != nil {
			log.Fatal(err)
		}
		ch <- thing
	}
}

func (nc *NatsClient) AddSubscription(mh nats.MsgHandler, subjectName string, opts []nats.SubOpt) {
	_, err := nc.Js.Subscribe(subjectName, mh, opts...)
	if err != nil {
		log.Fatal(err)
	}
}

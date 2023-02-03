package client

import (
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type NatsClient struct {
	Conn *nats.Conn
	Js   nats.JetStreamContext
}

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

func (nc *NatsClient) AddSubscriber(mh nats.MsgHandler, subjectName string) {
	log.Println("attempting to subscribe to", subjectName)
	_, err := nc.Js.Subscribe(subjectName, mh)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("subscribing to nats stream", subjectName)
}

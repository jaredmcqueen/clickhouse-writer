package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jaredmcqueen/clickhouse-writer/client"
	"github.com/jaredmcqueen/clickhouse-writer/instrument"
	"github.com/nats-io/nats.go"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	log.Println("starting clickhouse-writer")

	// catch control+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	flag.PrintDefaults()
	natsEndpoint := flag.String("natsEndpoint", "localhost:4222", "nats endpoint")
	clickhouseEndpoint := flag.String("clickhouseEndpoint", "localhost:9000", "Clickhouse endpoint")
	enableTrades := flag.Bool("trades", true, "enable trades")
	enableBars := flag.Bool("bars", true, "enable bars")
	enableQuotes := flag.Bool("quotes", true, "enable quotes")
	enableStatuses := flag.Bool("statuses", true, "enable trading statuses")
	startTime := flag.String("startTime", "earliest", "now, last, earliest")
	flag.Parse()

	clickhousePassword := os.Getenv("CLICKHOUSE_PASSWORD")

	// print all the variables
	log.Println("variables set:")
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("%s: %s (%s)", f.Name, f.Value, f.DefValue)
	})

	// connect to nats
	natsClient := client.NewNatsClient(*natsEndpoint)
	defer natsClient.Conn.Close()

	// connect to clickhouse
	chClient := client.NewClickhouseClient(*clickhouseEndpoint, clickhousePassword)
	defer func() {
		if err := chClient.Conn.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	if *enableTrades {
		Enable(instrument.Trade{}, instrument.ITrade, chClient, natsClient, *startTime)
	}

	if *enableQuotes {
		Enable(instrument.Quote{}, instrument.IQuote, chClient, natsClient, *startTime)
	}
	//
	if *enableBars {
		Enable(instrument.Bar{}, instrument.IBar, chClient, natsClient, *startTime)
	}

	if *enableStatuses {
		Enable(instrument.Status{}, instrument.IStatus, chClient, natsClient, *startTime)
	}

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}

func Enable[T any](t T, i instrument.Instrument, chc *client.ClickhouseClient, nc *client.NatsClient, st string) {
	instrumentChan := make(chan T)
	if err := chc.CreateTable(i.CreateSQL); err != nil {
		log.Fatal(err)
	}

	subOpts := []nats.SubOpt{}

	switch st {
	case "last":
		t := chc.GetLatestTimeStamp(i.TableName)
		log.Println("subscribing to nats subject", i.TableName, "starting at", t)
		subOpts = append(subOpts, nats.StartTime(t))
	case "now":
		t := time.Now()
		log.Println("subscribing to nats subject", i.TableName, "starting at", t)
		subOpts = append(subOpts, nats.StartTime(t))
	default:
		log.Println("subscribing to nats subject", i.TableName, "starting at earliest record")
	}

	chHandler := client.NewClickhouseWriterHandler(instrumentChan, i.TableName, i.InsertSQL)
	chc.AddClickhouseWriterHandler(chHandler)
	natsHandler := client.NewNatsHandler(instrumentChan)
	nc.AddSubscriber(natsHandler, i.TableName, subOpts)
}

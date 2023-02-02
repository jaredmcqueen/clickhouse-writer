package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/jaredmcqueen/clickhouse-writer/client"
	"github.com/jaredmcqueen/clickhouse-writer/instrument"
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
	clickhouseDatabase := flag.String("clickhouseDatabase", "default", "Clickhouse database")
	clickhouseUsername := flag.String("clickhouseUsername", "default", "Clickhouse username")
	enableTrades := flag.Bool("trades", true, "enable trades")
	enableBars := flag.Bool("bars", true, "enable bars")
	enableQuotes := flag.Bool("quotes", true, "enable quotes")
	enableStatuses := flag.Bool("statuses", true, "enable trading statuses")
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
	chClient := client.NewClickhouseClient(*clickhouseEndpoint, *clickhouseDatabase, *clickhouseUsername, clickhousePassword)
	defer func() {
		if err := chClient.Conn.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	if *enableTrades {
		Enable(instrument.Trade{}, instrument.ITrade, chClient, natsClient)
	}

	if *enableQuotes {
		Enable(instrument.Quote{}, instrument.IQuote, chClient, natsClient)
	}
	//
	if *enableBars {
		Enable(instrument.Bar{}, instrument.IBar, chClient, natsClient)
	}

	if *enableStatuses {
		Enable(instrument.Status{}, instrument.IStatus, chClient, natsClient)
	}

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}

func Enable[T any](t T, i instrument.Instrument, chc *client.ClickhouseClient, nc *client.NatsClient) {
	instrumentChan := make(chan T)
	if err := chc.CreateTable(i.CreateSQL); err != nil {
		log.Fatal(err, i.CreateSQL)
	}

	chHandler := client.NewClickhouseWriterHandler(instrumentChan, i.TableName, i.InsertSQL)
	chc.AddClickhouseWriterHandler(chHandler)
	natsHandler := client.NewNatsHandler(instrumentChan)
	nc.AddSubscriber(natsHandler, i.TableName)
}

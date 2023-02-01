package client

import (
	"context"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickhouseClient struct {
	Conn clickhouse.Conn
}

func NewClickhouseClient(endpoint, password string) *ClickhouseClient {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{endpoint},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: password,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	a, _ := conn.ServerVersion()
	log.Println("connected to clickhouse endpoint", endpoint)
	log.Println("clickhouse version:", a)

	chc := &ClickhouseClient{
		Conn: conn,
	}

	return chc
}

func (c *ClickhouseClient) CreateTable(table string) error {
	if err := c.Conn.Exec(context.Background(), table); err != nil {
		return err
	}
	return nil
}

type chWriterHandler func(context.Context, *ClickhouseClient)

func NewClickhouseWriterHandler[T any](ch chan T, tableName, tableInsert string) chWriterHandler {
	return func(ctx context.Context, c *ClickhouseClient) {
		batch, err := c.Conn.PrepareBatch(ctx, tableInsert)
		if err != nil {
			log.Fatal()
		}

		counter := 0
		sendFunc := func() {
			if r := batch.Send(); r != nil {
				log.Fatal(err)
			}
			batch, err = c.Conn.PrepareBatch(ctx, tableInsert)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("sent batch of", counter, tableName)
			counter = 0
		}

		batchTicker := time.NewTicker(1 * time.Second)

		for {
			select {
			case <-batchTicker.C:
				if counter == 0 {
					continue
				}
				sendFunc()
			case data := <-ch:
				counter++
				if err := batch.AppendStruct(&data); err != nil {
					log.Fatal(err)
				}
				if counter >= 25_000 {
					sendFunc()
				}
			}
		}
	}
}

func (c *ClickhouseClient) AddClickhouseWriterHandler(handler chWriterHandler) {
	ctx := context.Background()
	go handler(ctx, c)
}

func (nc *ClickhouseClient) GetLatestTimeStamp(tableName string) time.Time {
	row := nc.Conn.QueryRow(context.Background(), "SELECT max(Timestamp) FROM trades")
	var lastInsert time.Time
	if err := row.Scan(&lastInsert); err != nil {
		log.Fatal(err)
	}
	return lastInsert
}

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

func NewClickhouseClient(endpoint, database, username, password string) *ClickhouseClient {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{endpoint},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
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

func (c *ClickhouseClient) CreateTable(tableSQL, tableName string) {
	if err := c.Conn.Exec(context.Background(), tableSQL); err != nil {
		log.Fatal(err)
	}
	log.Println("created table", tableName)
}

type chWriterHandler func(context.Context, *ClickhouseClient)

func NewClickhouseWriterHandler[T any](ch chan T, tableName, tableInsert string, batchSize int, batchDuration int) chWriterHandler {
	return func(ctx context.Context, c *ClickhouseClient) {
		batch, err := c.Conn.PrepareBatch(ctx, tableInsert)
		if err != nil {
			log.Fatal()
		}

		counter := 0
		timeout := time.Duration(batchDuration)
		timer := time.NewTimer(timeout)

		sendFunc := func() {
			if r := batch.Send(); r != nil {
				// FIXME bad logging
				log.Println(batch)
				log.Fatal("error sending batch to clickhouse", err)
			}
			log.Println("sent batch of", counter, tableName)

			// get ready for the next batch
			batch, err = c.Conn.PrepareBatch(ctx, tableInsert)
			if err != nil {
				log.Fatal(err)
			}
			counter = 0
			timer.Reset(timeout)
		}

		for {
			select {
			case <-timer.C:
				if counter > 0 {
					sendFunc()
				}
			case data := <-ch:
				counter++
				if err := batch.AppendStruct(&data); err != nil {
					// FIXME: better logging
					log.Fatal("appending struct", &data, err)
				}

				if counter >= batchSize {
					if !timer.Stop() {
						<-timer.C
					}
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

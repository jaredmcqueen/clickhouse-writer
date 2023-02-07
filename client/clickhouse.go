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
		batchTicker := time.NewTicker(time.Duration(batchDuration) * time.Millisecond)

		sendFunc := func() {
			if r := batch.Send(); r != nil {
				log.Fatal(r)
			}
			batch, err = c.Conn.PrepareBatch(ctx, tableInsert)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("sent batch of", counter, tableName)
			counter = 0
			batchTicker.Reset(time.Duration(batchDuration) * time.Millisecond)
		}

		// BUG there is a race condition here
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
				if counter >= batchSize {
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

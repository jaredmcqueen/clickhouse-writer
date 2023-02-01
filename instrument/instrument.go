package instrument

import (
	"time"
)

type Trade struct {
	ID         int64
	Symbol     string
	Exchange   string
	Price      float64
	Size       uint32
	Timestamp  time.Time
	Conditions []string
	Tape       string
}

type Quote struct {
	Symbol      string
	BidExchange string
	BidPrice    float64
	BidSize     uint32
	AskExchange string
	AskPrice    float64
	AskSize     uint32
	Timestamp   time.Time
	Conditions  []string
	Tape        string
}

type Bar struct {
	Symbol     string
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     uint64
	Timestamp  time.Time
	TradeCount uint64
	VWAP       float64
}

type Status struct {
	Symbol     string
	StatusCode string
	StatusMsg  string
	ReasonCode string
	ReasonMsg  string
	Timestamp  time.Time
	Tape       string
}

type Instrument struct {
	TableName string
	CreateSQL string
	InsertSQL string
}

var ITrade Instrument = Instrument{
	TableName: "trades",
	CreateSQL: `
		CREATE TABLE IF NOT EXISTS trades (
  #  ID Int64
			 Symbol String
			, Exchange String
			, Price Float64
			, Size UInt32
			, Timestamp DateTime
			, Conditions Array(String)
			, Tape String
		) Engine = MergeTree()
    ORDER BY Symbol
  `,
	InsertSQL: "INSERT INTO trades",
}

var IQuote Instrument = Instrument{
	TableName: "quotes",
	CreateSQL: `
		CREATE TABLE IF NOT EXISTS quotes (
      Symbol      String
    , BidExchange String
    , BidPrice    Float64
    , BidSize     UInt32
    , AskExchange String
    , AskPrice    Float64
    , AskSize     UInt32
    , Timestamp   DateTime
    , Conditions  Array(String)
    , Tape        String
		) Engine = MergeTree()
    ORDER BY Symbol
  `,
	InsertSQL: "INSERT INTO quotes",
}

var IBar Instrument = Instrument{
	TableName: "bars",
	CreateSQL: `
		CREATE TABLE IF NOT EXISTS bars (
      Symbol     String
    , Open       Float64
    , High       Float64
    , Low        Float64
    , Close      Float64
    , Volume     UInt64
    , Timestamp  DateTime
    , TradeCount UInt64
    , VWAP       Float64
		) Engine = MergeTree()
    ORDER BY Symbol
  `,
	InsertSQL: "INSERT INTO bars",
}

var IStatus Instrument = Instrument{
	TableName: "statuses",
	CreateSQL: `
		CREATE TABLE IF NOT EXISTS statuses (
      Symbol     String
    , StatusCode String
    , StatusMsg  String
    , ReasonCode String
    , ReasonMsg  String
    , Timestamp  DateTime
    , Tape       String
		) Engine = MergeTree()
    ORDER BY Symbol
  `,
	InsertSQL: "INSERT INTO statuses",
}
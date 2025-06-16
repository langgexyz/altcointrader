package db

import (
	"context"

	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type KlineInterval = string

const (
	KlineInterval1D = "1d"
)

type KlineDocument struct {
	ID                  string `bson:"_id"`
	Interval            KlineInterval
	OpenTime            int64   // 开盘时间（毫秒时间戳）
	Open                float64 // 开盘价
	High                float64 // 最高价
	Low                 float64 // 最低价
	Close               float64 // 收盘价
	Volume              float64 // 成交量（以基础资产计）
	CloseTime           int64   // 收盘时间（毫秒时间戳）
	QuoteAssetVolume    float64 // 成交额（以计价资产计）
	TradeCount          int     // 成交笔数
	TakerBuyBaseVolume  float64 // 主动买入成交量（基础币）
	TakerBuyQuoteVolume float64 // 主动买入成交额（计价币）
	IsOldest            bool    // 是否为历史数据同步的最后一条数据
}

func (col *Kline) field() *KlineDocument0Field {
	return NewKlineDocument0Field("")
}

func (col *Kline) collection() *mongo.Collection {
	const colName = "Kline"
	return mongocache.MustGet(col.ctx, ConfigValue.Config).Database(ConfigValue.DBName).Collection(colName)
}

type Kline struct {
	ctx    context.Context
	logger *log.Logger
}

func (col *Kline) Insert(doc *KlineDocument) error {
	_, err := col.collection().InsertOne(col.ctx, doc)
	return err
}

func (col *Kline) InsertMany(docs []*KlineDocument) error {
	if len(docs) == 0 {
		return nil
	}
	interfaceDocs := make([]interface{}, len(docs))
	for i, doc := range docs {
		interfaceDocs[i] = doc
	}
	_, err := col.collection().InsertMany(col.ctx, interfaceDocs)
	return err
}

func NewKline(ctx context.Context) *Kline {
	_, logger := log.WithCtx(ctx)
	logger.PushPrefix("Kline")
	return &Kline{
		ctx:    ctx,
		logger: logger,
	}
}

// GetEarliestKline 获取指定交易对和周期的最早K线数据（按时间排序）
func (col *Kline) GetEarliestKline(interval KlineInterval) (doc *KlineDocument, err error) {
	opts := options.FindOne().SetSort(bson.D{{col.field().OpenTime().FullName(), 1}})

	doc = &KlineDocument{}
	err = col.collection().FindOne(col.ctx, col.field().Interval().Eq(interval).ToBsonD(), opts).Decode(&doc)

	return doc, err
}

func (col *Kline) GetOldestKline(interval KlineInterval) (doc *KlineDocument, err error) {
	opts := options.FindOne().SetSort(bson.D{{col.field().OpenTime().FullName(), -1}})

	doc = &KlineDocument{}
	err = col.collection().FindOne(col.ctx, col.field().Interval().Eq(interval).ToBsonD(), opts).Decode(&doc)
	return doc, err
}

func (col *Kline) SetOldestFlag(id string) (err error) {
	_, err = col.collection().UpdateOne(col.ctx, col.field().ID().Eq(id).ToBsonD(),
		col.field().IsOldest().Set(true))
	if err != nil {
		return err
	}

	return nil
}

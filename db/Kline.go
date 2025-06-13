package db

import (
	"context"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/mongo"
)

type KlineDocument struct {
	ID                  string `bson:"_id"`
	Interval            string
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

func NewKline(ctx context.Context) *Kline {
	_, logger := log.WithCtx(ctx)
	logger.PushPrefix("Kline")
	return &Kline{
		ctx:    ctx,
		logger: logger,
	}
}

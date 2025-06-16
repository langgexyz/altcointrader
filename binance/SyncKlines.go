package binance

import (
	"altcointrader/db"
	"context"
	"time"

	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// 每次请求的K线数量
	defaultLimit = 500
	// 请求间隔时间（毫秒）
	requestInterval = 1000
)

// Sync1DKlineToOldest 同步历史数据
func Sync1DKlineToOldest(ctx context.Context, symbol string) error {
	ctx, logger := log.WithCtx(ctx)
	interval := db.KlineInterval1D
	oldest, err := db.NewKline(ctx).GetOldestKline(interval)
	endTime := time.Now().Truncate(24 * time.Hour).UnixMilli()

	if err != nil {
		if err != mongo.ErrNoDocuments {
			return err
		}
	}

	if oldest.IsOldest {
		return nil
	}

	endTime = oldest.OpenTime

	for {
		// 获取K线数据
		docs, err := GetLines(ctx, symbol, interval, 0, endTime, defaultLimit)
		if err != nil {
			logger.Error("Failed to get klines: " + err.Error())
			return err
		}

		if len(docs) == 0 {
			return db.NewKline(ctx).SetOldestFlag(oldest.ID)
		}

		err = db.NewKline(ctx).InsertMany(docs)
		if err != nil {
			return err
		}

		if len(docs) < defaultLimit {
			// TODO GetLines 保证顺序，但是不知道是增序、还是降序
			if docs[0].OpenTime < docs[len(docs)-1].OpenTime {
				return db.NewKline(ctx).SetOldestFlag(docs[0].ID)
			} else {
				return db.NewKline(ctx).SetOldestFlag(docs[len(docs)-1].ID)
			}
		}
		time.Sleep(time.Duration(requestInterval) * time.Millisecond)
	}
}

// Sync1DKlineToEarliest 同步增量数据
func Sync1DKlineToEarliest(ctx context.Context, symbol string) error {
	ctx, logger := log.WithCtx(ctx)
	interval := db.KlineInterval1D
	earliest, err := db.NewKline(ctx).GetEarliestKline(interval)
	if err != nil {
		return err
	}

	startTime := earliest.CloseTime + 1

	for {
		// 获取K线数据
		docs, err := GetLines(ctx, symbol, interval, startTime, time.Now().UnixMilli(), defaultLimit)
		if err != nil {
			logger.Error("Failed to get klines: " + err.Error())
			return err
		}

		if len(docs) == 0 {
			return nil
		}

		err = db.NewKline(ctx).InsertMany(docs)
		if err != nil {
			return err
		}

		time.Sleep(time.Duration(requestInterval) * time.Millisecond)
	}
}

func Sync1DKline(ctx context.Context, symbol string) error {
	if err := Sync1DKlineToOldest(ctx, symbol); err != nil {
		return err
	}

	return Sync1DKlineToEarliest(ctx, symbol)
}

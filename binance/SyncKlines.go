package binance

import (
	"altcointrader/db"
	"context"
	"fmt"
	"time"

	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// 每次请求的K线数量
	defaultLimit = 500
	// 请求间隔时间（毫秒）
	requestInterval = 1000
)

// syncHistoricalKlines 同步历史数据
func syncHistoricalKlines(ctx context.Context, klineDB *db.Kline, symbol string, logger *log.Logger) error {
	// 获取已同步的最早数据
	oldestKline, err := klineDB.GetOldestKline("1d")
	if err != nil {
		logger.Error("Failed to get oldest kline: " + err.Error())
		return err
	}

	var startTime int64
	if oldestKline == nil {
		// 如果没有数据，从一年前开始同步
		startTime = time.Now().AddDate(-1, 0, 0).UnixMilli()
		logger.Info("No existing data found, will sync from one year ago")
	} else {
		// 从最早数据的前一天开始同步
		startTime = oldestKline.OpenTime - 24*60*60*1000 // 减去一天的毫秒数
		logger.Info(fmt.Sprintf("Found oldest kline at %d, will sync from %d", oldestKline.OpenTime, startTime))
	}

	endTime := time.Now().UnixMilli()
	currentEndTime := endTime

	totalSynced := 0
	for {
		// 获取K线数据
		klines, err := GetLines(ctx, symbol, "1d", startTime, currentEndTime, defaultLimit)
		if err != nil {
			logger.Error("Failed to get klines: " + err.Error())
			return err
		}

		if len(klines) == 0 {
			if currentEndTime <= startTime {
				break
			}
			currentEndTime = startTime
			continue
		}

		// 批量插入数据
		inserted := 0
		for _, kline := range klines {
			if err := klineDB.Insert(kline); err != nil {
				if err.Error() == "duplicate key error" {
					continue
				}
				logger.Error("Failed to insert kline: " + err.Error())
				return err
			}
			inserted++
		}

		totalSynced += inserted
		logger.Info(fmt.Sprintf("Historical sync: synced %d new klines, total: %d", inserted, totalSynced))

		if len(klines) < defaultLimit {
			if currentEndTime <= startTime {
				// 标记最后一条数据为最早数据
				if len(klines) > 0 {
					if err := klineDB.UpdateOldestFlag("1d", klines[0].OpenTime); err != nil {
						logger.Error("Failed to update oldest flag: " + err.Error())
						return err
					}
				}
				break
			}
			currentEndTime = klines[0].OpenTime - 1
			continue
		}

		currentEndTime = klines[0].OpenTime - 1
		time.Sleep(time.Duration(requestInterval) * time.Millisecond)
	}

	logger.Info(fmt.Sprintf("Historical sync completed, total synced: %d klines", totalSynced))
	return nil
}

// syncIncrementalKlines 同步增量数据
func syncIncrementalKlines(ctx context.Context, klineDB *db.Kline, symbol string, logger *log.Logger) error {
	// 获取数据库中最新的K线数据（按OpenTime降序排序的第一条）
	opts := options.FindOne().SetSort(bson.D{{"openTime", -1}})
	var latestKline db.KlineDocument
	err := klineDB.collection().FindOne(ctx, bson.M{
		"interval": "1d",
		"_id": bson.M{
			"$regex": "^1d",
		},
	}, opts).Decode(&latestKline)

	var startTime int64
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// 如果没有数据，从今天开始
			startTime = time.Now().Truncate(24 * time.Hour).UnixMilli()
			logger.Info("No existing data found, will sync from today")
		} else {
			logger.Error("Failed to get latest kline: " + err.Error())
			return err
		}
	} else {
		// 从最新数据的下一天开始同步
		startTime = latestKline.CloseTime + 1
		logger.Info(fmt.Sprintf("Found latest kline at %d, will sync from %d", latestKline.CloseTime, startTime))
	}

	// 获取当前时间作为结束时间
	endTime := time.Now().UnixMilli()

	// 如果开始时间已经超过结束时间，说明数据已经是最新的
	if startTime >= endTime {
		logger.Info("Data is already up to date")
		return nil
	}

	// 获取K线数据
	klines, err := GetLines(ctx, symbol, "1d", startTime, endTime, defaultLimit)
	if err != nil {
		logger.Error("Failed to get klines: " + err.Error())
		return err
	}

	if len(klines) == 0 {
		logger.Info("No new data to sync")
		return nil
	}

	// 批量插入数据
	for _, kline := range klines {
		if err := klineDB.Insert(kline); err != nil {
			if err.Error() == "duplicate key error" {
				continue
			}
			logger.Error("Failed to insert kline: " + err.Error())
			return err
		}
	}

	logger.Info(fmt.Sprintf("Incremental sync completed, synced %d new klines", len(klines)))
	return nil
}

// SyncDailyKlines 同步指定交易对的日K线数据到数据库
// 分为两部分：历史数据同步和增量同步
// symbol: 交易对名称，例如 "BTCUSDT"
func SyncDailyKlines(ctx context.Context, symbol string) error {
	_, logger := log.WithCtx(ctx)
	logger.PushPrefix("SyncDailyKlines")

	// 创建数据库操作对象
	klineDB := db.NewKline(ctx)

	// 检查是否有历史数据标记
	oldestKline, err := klineDB.GetOldestKline("1d")
	if err != nil {
		logger.Error("Failed to check oldest kline: " + err.Error())
		return err
	}

	// 如果没有历史数据标记，先同步历史数据
	if oldestKline == nil {
		logger.Info("Starting historical data sync")
		if err := syncHistoricalKlines(ctx, klineDB, symbol, logger); err != nil {
			return err
		}
	}

	// 同步增量数据
	logger.Info("Starting incremental sync")
	return syncIncrementalKlines(ctx, klineDB, symbol, logger)
}

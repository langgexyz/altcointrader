package binance

import (
	"altcointrader/db"
	"context"
	"fmt"
	"github.com/xpwu/go-log/log"
	"time"
)

const (
	// 每次请求的K线数量
	defaultLimit = 500
	// 请求间隔时间（毫秒）
	requestInterval = 1000
)

// SyncDailyKlines 同步指定交易对的日K线数据到数据库
// 会从数据库中查询最新的K线数据，然后同步到当前时间
// symbol: 交易对名称，例如 "BTCUSDT"
func SyncDailyKlines(ctx context.Context, symbol string) error {
	_, logger := log.WithCtx(ctx)
	logger.PushPrefix("SyncDailyKlines")

	// 创建数据库操作对象
	klineDB := db.NewKline(ctx)

	// 查询最新的K线数据
	latestKline, err := klineDB.GetLatestKline("1d")
	if err != nil {
		logger.Error("Failed to query latest kline: " + err.Error())
		return err
	}

	var startTime int64
	if latestKline == nil {
		// 如果没有数据，从一年前开始同步
		startTime = time.Now().AddDate(-1, 0, 0).UnixMilli()
		logger.Info("No existing data found, will sync from one year ago")
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

	totalSynced := 0
	for {
		// 获取K线数据
		klines, err := GetLines(ctx, symbol, "1d", startTime, endTime, defaultLimit)
		if err != nil {
			logger.Error("Failed to get klines: " + err.Error())
			return err
		}

		if len(klines) == 0 {
			logger.Info("No more data to sync")
			break
		}

		// 批量插入数据
		for _, kline := range klines {
			if err := klineDB.Insert(kline); err != nil {
				logger.Error("Failed to insert kline: " + err.Error())
				return err
			}
		}

		totalSynced += len(klines)
		logger.Info(fmt.Sprintf("Synced %d klines, total: %d", len(klines), totalSynced))

		// 如果返回的数据量小于limit，说明已经同步到最新数据
		if len(klines) < defaultLimit {
			logger.Info("Reached latest data")
			break
		}

		// 更新开始时间为最后一条数据的结束时间
		startTime = klines[len(klines)-1].CloseTime + 1

		// 如果开始时间已经超过结束时间，说明数据已经是最新的
		if startTime >= endTime {
			logger.Info("Reached current time")
			break
		}

		// 控制请求频率
		time.Sleep(time.Duration(requestInterval) * time.Millisecond)
	}

	logger.Info(fmt.Sprintf("Successfully completed sync, total synced: %d klines for %s", totalSynced, symbol))
	return nil
} 
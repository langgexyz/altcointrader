package binance

import (
	"altcointrader/db"
	"context"
	"github.com/xpwu/go-log/log"
)

// SyncDailyKlines 同步指定交易对的日K线数据到数据库
// symbol: 交易对名称，例如 "BTCUSDT"
// limit: 获取的K线数量，最大1000
func SyncDailyKlines(ctx context.Context, symbol string, limit int) error {
	_, logger := log.WithCtx(ctx)
	logger.PushPrefix("SyncDailyKlines")

	// 获取K线数据
	klines, err := GetLines(ctx, symbol, "1d", limit)
	if err != nil {
		logger.Error("Failed to get klines: " + err.Error())
		return err
	}

	// 创建数据库操作对象
	klineDB := db.NewKline(ctx)

	// 批量插入数据
	for _, kline := range klines {
		if err := klineDB.Insert(kline); err != nil {
			logger.Error("Failed to insert kline: " + err.Error())
			return err
		}
	}

	logger.Info("Successfully synced " + symbol + " daily klines")
	return nil
} 
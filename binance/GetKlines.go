package binance

import (
	"altcointrader/db"
	"context"
	"encoding/json"
	"fmt"
	"github.com/xpwu/go-httpclient/httpc"
	"github.com/xpwu/go-log/log"
	"strconv"
)

func GetLines(ctx context.Context, symbol string, interval string, limit int) (docs []*db.KlineDocument, err error) {
	_, logger := log.WithCtx(ctx)
	url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=%d", symbol, interval, limit)

	var body = make([]byte, 0)
	err = httpc.Send(ctx, url, httpc.WithBytesResponse(&body))

	var rawData [][]interface{}
	if err := json.Unmarshal(body, &rawData); err != nil {
		logger.Error(fmt.Sprintf("json unmarshal err:%+v", err))
		return nil, err
	}

	for _, item := range rawData {
		openTime := int64(item[0].(float64))
		open, _ := strconv.ParseFloat(item[1].(string), 64)
		high, _ := strconv.ParseFloat(item[2].(string), 64)
		low, _ := strconv.ParseFloat(item[3].(string), 64)
		closePrice, _ := strconv.ParseFloat(item[4].(string), 64)
		volume, _ := strconv.ParseFloat(item[5].(string), 64)
		closeTime := int64(item[6].(float64))
		quoteVolume, _ := strconv.ParseFloat(item[7].(string), 64)
		tradeCount := int(item[8].(float64))
		takerBuyBase, _ := strconv.ParseFloat(item[9].(string), 64)
		takerBuyQuote, _ := strconv.ParseFloat(item[10].(string), 64)

		kline := &db.KlineDocument{
			ID:                  fmt.Sprintf("%s%d", interval, openTime),
			Interval:            interval,
			OpenTime:            openTime,
			Open:                open,
			High:                high,
			Low:                 low,
			Close:               closePrice,
			Volume:              volume,
			CloseTime:           closeTime,
			QuoteAssetVolume:    quoteVolume,
			TradeCount:          tradeCount,
			TakerBuyBaseVolume:  takerBuyBase,
			TakerBuyQuoteVolume: takerBuyQuote,
		}
		docs = append(docs, kline)
	}

	return docs, nil
}

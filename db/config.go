package db

import (
	"github.com/xpwu/go-config/configs"
	mongoConfig "github.com/xpwu/go-db-mongo/mongodb/mongocache"
)

type config struct {
	mongoConfig.Config
	DBName string
}

var ConfigValue = config{
	Config: mongoConfig.Config{
		URI:      "",
		User:     "",
		Password: "",
		MaxConn:  0,
	},
	DBName: "",
}

func init() {
	configs.Unmarshal(&ConfigValue)
}

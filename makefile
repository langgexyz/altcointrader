pwd := $(shell pwd)

.PHONY: prom push

prom:
	@echo "usage: make build, make dev, make dep"

build:
	@export GOPROXY=https://goproxy.io,direct && go mod tidy && go build -o ./bin/go_build_altcointrader_server_main ./main/main.go
.PHONY:build

debug:
	@export GOPROXY=https://goproxy.io,direct && go mod tidy && go build -o ./bin/go_build_altcointrader_server_main ./main/main.go; \
	cd bin; \
	if [ -f ./pid ]; then \
		kill -TERM `cat ./pid`; \
	fi; \
	nohup ./go_build_altcointrader_server_main -c ./config.json > ./error.log 2>&1 & echo $$! > pid; \
	cd ..; \
	echo "make debug end";
.PHONY:debug

dep:
	@export GOPROXY=https://goproxy.io,direct && cd src && go list -m all

sendfile:
	@scp allCVTList.csv FollowUnFollow.csv og_claim_info.xlsx gcp-depth-server:/home/jingxu
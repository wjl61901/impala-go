.PHONY: thrift
thrift:
	thrift -r -gen go:package_prefix=github.com/sclgo/impala-go/services/ interfaces/ImpalaService.thrift
	rm -rf ./services
	mv gen-go services

.PHONY: cli
cli: usql

usql:
	go run github.com/sclgo/usqlgen@v0.1.1 -v build --import github.com/sclgo/impala-go

.PHONY: test-cli
test-cli: usql
	./usql -c "\drivers" | grep impala



thrift:
	thrift -r -gen go:package_prefix=github.com/sclgo/impala-go/services/ interfaces/ImpalaService.thrift
	rm -rf ./services
	mv gen-go services

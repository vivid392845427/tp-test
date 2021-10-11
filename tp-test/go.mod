module github.com/pingcap/go-randgen/tp-test

go 1.16

require (
	github.com/go-sql-driver/mysql v1.5.0
	github.com/google/uuid v1.1.1
	github.com/pingcap/go-randgen v0.0.0-20200721020841-1466757857eb
	github.com/spf13/cobra v1.0.0
	github.com/yuin/gopher-lua v0.0.0-20190514113301-1cd887cd7036
	github.com/zyguan/sqlz v0.0.0-20201223085349-8d496c5b18b2
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
)

replace github.com/pingcap/go-randgen => ../

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pingcap/go-randgen/tp-test/gen"

	. "github.com/zyguan/just"
)

func main() {
	defer Catch(func(c Catchable) {
		fmt.Fprintf(os.Stderr, "\x1b[0;31mError: %+v\x1b[0m\n", c.Why())
	})
	var (
		opts  gen.GenTestOptions
		input string
	)
	flag.StringVar(&input, "i", "", "input grammar file")
	flag.StringVar(&opts.InitRoot, "init-root", "init", "init root")
	flag.StringVar(&opts.TxnRoot, "txn-root", "txn", "txn root")
	flag.IntVar(&opts.TxnCount, "n", 2, "txn count")
	flag.IntVar(&opts.RecurLimit, "limit", 15, "recursive limit")
	flag.BoolVar(&opts.Debug, "debug", false, "enable debug")
	flag.Parse()

	f := os.Stdin
	if len(input) > 0 {
		f = Try(os.Open(input)).(*os.File)
		defer f.Close()
	}
	raw := Try(ioutil.ReadAll(f)).([]byte)
	opts.Grammar = string(raw)

	t := Try(gen.GenTest(opts)).(gen.Test)
	for _, q := range t.Init {
		fmt.Printf("/* INIT */ %s;\n", q)
	}
	for i, t := range t.TxnList {
		for _, q := range t {
			fmt.Printf("/* T%-3d */ %s;\n", i, q)
		}
	}
}

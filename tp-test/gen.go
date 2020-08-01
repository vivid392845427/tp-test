package main

import (
	"math/rand"

	"github.com/pingcap/go-randgen/grammar"
	"github.com/pingcap/go-randgen/grammar/sql_generator"
)

type genTestOptions struct {
	Grammar      string
	Root         string
	MaxRecursion int
	NumTxn       int
	MinStmt      int
	MaxStmt      int
	Debug        bool
	Inits        []Init
}

func genTest(opts genTestOptions) (test Test, err error) {
	if opts.MinStmt > opts.MaxStmt {
		opts.MinStmt, opts.MaxStmt = opts.MaxStmt, opts.MinStmt
	}
	test.Init = &opts.Inits[rand.Intn(len(opts.Inits))]

	it, err := grammar.NewIter(opts.Grammar, opts.Root, opts.MaxRecursion, nil, opts.Debug)
	if err != nil {
		return Test{}, err
	}
	for i := 0; i < opts.NumTxn; i++ {
		k := opts.MinStmt + rand.Intn(opts.MaxStmt-opts.MinStmt+1)
		txn := make(Txn, 0, k)
		err = it.Visit(sql_generator.FixedTimesVisitor(func(_ int, sql string) {
			txn = append(txn, Stmt{Stmt: sql})
		}, k))
		if err != nil {
			return Test{}, err
		}
		test.Steps = append(test.Steps, txn)
	}
	return
}

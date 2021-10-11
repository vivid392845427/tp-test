package main

import (
	"strings"
)

const (
	TestPending = "Pending"
	TestRunning = "Running"
	TestFailed  = "Failed"
	TestPassed  = "Passed"
	TestUnknown = "Unknown"
)

type Test struct {
	ID         string
	Status     string
	Message    string
	StartedAt  int64
	FinishedAt int64
	InitSQL    []string
	Groups     []StmtList
}

type StmtList []Stmt

type Stmt struct {
	Seq     int
	Txn     int
	TestID  string
	Stmt    string
	IsQuery bool
}

func naiveQueryDetect(sql string) bool {
	sql = strings.ToLower(strings.TrimLeft(strings.TrimSpace(sql), "("))
	for _, w := range []string{"select ", "show ", "admin show ", "explain ", "desc ", "describe "} {
		if strings.HasPrefix(sql, w) {
			return true
		}
	}
	return false
}

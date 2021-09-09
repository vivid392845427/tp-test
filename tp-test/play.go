package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/zyguan/sqlz"
	"github.com/zyguan/sqlz/resultset"
	"golang.org/x/sync/errgroup"
)

type playOptions struct {
	genTestOptions
	DSN1    string
	DSN2    string
	DBName  string
	OutDir  string
	Tests   int64
	Threads int
}

func play(opts playOptions) error {
	var (
		cnt int64
		g   errgroup.Group
	)
	openDB := func(dsn string, tag string, i int) (*sql.DB, error) {
		ctl, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}
		defer ctl.Close()

		cfg, _ := mysql.ParseDSN(dsn)
		cfg.DBName = fmt.Sprintf("%s_%s_%02d", opts.DBName, tag, i)

		if _, err = ctl.Exec("create database if not exists " + cfg.DBName); err != nil {
			return nil, err
		}
		return sql.Open("mysql", cfg.FormatDSN())
	}
	initTest := func(test Test) Test {
		test.ID = strconv.FormatInt(time.Now().Unix(), 10) + "." + uuid.New().String()
		seq := 0
		for id, txn := range test.Groups {
			for k, stmt := range txn {
				stmt.TestID = test.ID
				stmt.Txn = id
				stmt.Seq = seq
				stmt.IsQuery = naiveQueryDetect(stmt.Stmt)
				txn[k] = stmt
				seq += 1
			}
		}
		return test
	}
	for i := 0; i < opts.Threads; i++ {
		db1, err := openDB(opts.DSN1, "dsn1", i)
		if err != nil {
			return err
		}
		db2, err := openDB(opts.DSN2, "dsn2", i)
		if err != nil {
			return err
		}
		g.Go(func() error {
			var firstErr error
			for atomic.AddInt64(&cnt, 1) <= opts.Tests {
				test, err := genTest(opts.genTestOptions)
				if err != nil {
					log.Printf("failed to gen test: %+v", err)
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				test = initTest(test)
				err = runTest(context.TODO(), test, db1, db2)
				if err != nil {
					log.Printf("failed to run test: %+v", err)
					if firstErr == nil {
						firstErr = err
					}
					dumpTest(opts, test, err)
					continue
				}
			}
			return firstErr
		})
	}
	return g.Wait()
}

func runTest(ctx context.Context, test Test, db1 *sql.DB, db2 *sql.DB) error {
	var (
		wg       sync.WaitGroup
		c1       *sql.Conn
		c2       *sql.Conn
		initConn = func(db *sql.DB, tag string) *sql.Conn {
			c, err := sqlz.Connect(ctx, db)
			if err != nil {
				log.Printf("%s> make connection: %+v", tag, err)
				return nil
			}
			for _, stmt := range test.InitSQL {
				if _, err = c.ExecContext(ctx, stmt); err != nil {
					log.Printf("%s> init stmt failed: %v @ %s", tag, err, stmt)
				}
			}
			return c
		}
		dropConn = func(c *sql.Conn) {
			if c == nil {
				return
			}
			c.Raw(func(driverConn interface{}) error {
				// force disconnect physical connection
				if dc, ok := driverConn.(io.Closer); ok {
					return dc.Close()
				}
				return nil
			})
			c.Close()
		}
	)
	defer func() {
		dropConn(c1)
		dropConn(c2)
	}()

	// init
	wg.Add(2)
	go func() {
		defer wg.Done()
		c1 = initConn(db1, "dsn1")
	}()
	go func() {
		defer wg.Done()
		c2 = initConn(db2, "dsn2")
	}()
	wg.Wait()
	if c1 == nil || c2 == nil {
		return fmt.Errorf("init test #%s failed", test.ID)
	}

	log.Printf("run test #%s", test.ID)
	for _, txn := range test.Groups {
		// submit transaction
		var (
			hs1  map[string]string
			hs2  map[string]string
			err1 error
			err2 error
		)

		for _, stmt := range txn {
			var (
				rs1 *resultset.ResultSet
				rs2 *resultset.ResultSet
			)

			// execute statement
			wg.Add(2)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				rs1, err1 = doStmt(ctx, c1, stmt)
				cancel()
			}()
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				rs2, err2 = doStmt(ctx, c2, stmt)
				cancel()
			}()
			wg.Wait()

			// validate result
			if !validateErrs(err1, err2) {
				return fmt.Errorf("errors mismatch: %v <> %v @(%s,%d) %q", err1, err2, test.ID, stmt.Seq, stmt.Stmt)
			}
			if rs1 == nil || rs2 == nil {
				log.Printf("skip same query error: [%v] [%v] @(%s,%d)", err1, err2, test.ID, stmt.Seq)
				continue
			}
			cellFilter := func(i int, j int, raw []byte) bool {
				if strings.Contains(stmt.Stmt, "union") {
					typ := rs1.ColumnDef(j).Type
					return typ != "FLOAT" && typ != "DOUBLE" && typ != "DECIMAL"
				}
				return true
			}
			h1, h2 := "", ""
			if q := strings.ToLower(stmt.Stmt); stmt.IsQuery && rs1.NRows() == rs2.NRows() && rs1.NRows() > 1 &&
				!strings.Contains(q, "diff-without-sort") {
				h1, h2 = rs1.UnorderedDigest(cellFilter), rs2.UnorderedDigest(cellFilter)
			} else {
				h1, h2 = rs1.DataDigest(cellFilter), rs2.DataDigest(cellFilter)
			}

			if h1 != h2 {
				return &ErrResultMismatch{stmt, rs1, rs2}
			}
		}
		// post check
		wg.Add(2)
		go func() {
			defer wg.Done()
			hs1, err1 = checkTables(ctx, db1, "")
		}()
		go func() {
			defer wg.Done()
			hs2, err2 = checkTables(ctx, db2, "")
		}()
		wg.Wait()

		if err1 != nil || err2 != nil {
			return fmt.Errorf("post check tables failed with errors: %v <> %v", err1, err2)
		}
		for t := range hs2 {
			if hs1[t] != hs2[t] {
				return fmt.Errorf("data mismatch @%s", t)
			}
		}
	}

	return nil
}

func dumpTest(opts playOptions, test Test, err error) {
	name := filepath.Join(opts.OutDir, test.ID+".txt")
	log.Printf("dump failure info to " + name)
	os.MkdirAll(opts.OutDir, 0755)
	f, e := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if e != nil {
		log.Printf("cannot open file: %v", e)
		return
	}
	defer f.Close()
	for _, stmt := range test.InitSQL {
		fmt.Fprintln(f, "/* INIT */ "+stmt+";")
	}
	for _, txn := range test.Groups {
		for _, stmt := range txn {
			fmt.Fprintf(f, "/* T%03d_%03d */ %s;\n", stmt.Txn, stmt.Seq, stmt.Stmt)
		}
	}
	fmt.Fprintln(f, "---------")
	fmt.Fprintln(f, err.Error())
}

type ErrResultMismatch struct {
	stmt Stmt
	rs1  *resultset.ResultSet
	rs2  *resultset.ResultSet
}

func (e *ErrResultMismatch) Error() string {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "result mismatch @(%s,%d) %q\n", e.stmt.TestID, e.stmt.Seq, e.stmt.Stmt)
	fmt.Fprintln(buf, "<<<")
	e.rs1.PrettyPrint(buf)
	fmt.Fprintln(buf, "===")
	e.rs2.PrettyPrint(buf)
	fmt.Fprintln(buf, ">>>")
	return buf.String()
}

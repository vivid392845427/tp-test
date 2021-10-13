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
	"github.com/olekukonko/tablewriter"
	"github.com/pingcap/go-randgen/grammar/sqlgen"
	"github.com/zyguan/sqlz"
	"golang.org/x/sync/errgroup"
)

type playOptions struct {
	genTestOptions
	DSN1    string
	DSN2    string
	DBName  string
	OutDir  string
	Rounds  int64
	Threads int
	DryRun  bool
}

func play(opts playOptions) error {
	var (
		cnt int64
		g   errgroup.Group
	)
	if opts.DryRun {
		for i := int64(0); i < opts.Rounds; i++ {
			fmt.Fprintf(os.Stdout, "-- ROUND %d\n", i)
			t, err := genTest(opts.genTestOptions)
			if err != nil {
				return err
			}
			dumpTest(os.Stdout, opts, t, nil)
		}
		return nil
	}
	openDB := func(dsn string, tag string, i int) (*sql.DB, error) {
		ctl, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}
		defer ctl.Close()

		cfg, _ := mysql.ParseDSN(dsn)
		cfg.DBName = fmt.Sprintf("%s_%s_%02d", opts.DBName, tag, i)
		if _, err = ctl.Exec("drop database if exists " + cfg.DBName); err != nil {
			return nil, err
		}
		if _, err = ctl.Exec("create database " + cfg.DBName); err != nil {
			return nil, err
		}
		finalDSN := cfg.FormatDSN()
		log.Printf("[%s:%d] open db: %s", tag, i, finalDSN)
		return sql.Open("mysql", finalDSN)
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
			for atomic.AddInt64(&cnt, 1) <= opts.Rounds {
				test, err := genTest(opts.genTestOptions)
				if err != nil {
					log.Printf("failed to gen test: %+v", err)
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				err = runTest(context.TODO(), test, db1, db2)
				if err != nil {
					log.Printf("failed to run test: %+v", err)
					if firstErr == nil {
						firstErr = err
					}
					dumpTest(nil, opts, test, err)
					continue
				}
			}
			return firstErr
		})
	}
	return g.Wait()
}

func runTest(ctx context.Context, round TestRound, db1 *sql.DB, db2 *sql.DB) error {
	var (
		wg       sync.WaitGroup
		c1       *sql.Conn
		c2       *sql.Conn
		initConn = func(db *sql.DB, tag string) *sql.Conn {
			var (
				c   *sql.Conn
				err error
			)
			for {
				c, err = db.Conn(ctx)
				if err != nil {
					log.Printf("%s> make connection: %+v", tag, err)
					return nil
				}
				if err := c.PingContext(ctx); err == nil {
					break
				}
				c.Close()
			}
			for _, stmt := range round.Init {
				if _, err = c.ExecContext(ctx, stmt.Query, stmt.Params...); err != nil {
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
		return fmt.Errorf("init round #%s failed", round.ID)
	}
	c1p := sqlz.WithStmtCache(c1)
	c2p := sqlz.WithStmtCache(c2)

	seq := 0
	log.Printf("start round #%s", round.ID)
	for _, test := range round.Tests {
		// submit transaction
		var (
			hs1  map[string]string
			hs2  map[string]string
			err1 error
			err2 error
		)

		for _, stmt := range test {
			var (
				rs1 *sqlz.ResultSet
				rs2 *sqlz.ResultSet
			)
			seq++
			stmtID := round.ID + ":" + strconv.Itoa(seq)
			// execute statement
			wg.Add(2)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				if stmt.Flags&sqlgen.STMT_PREPARED > 0 {
					rs1, err1 = doStmt(ctx, c1p, round.ID, stmt)
				} else {
					rs1, err1 = doStmt(ctx, c1, round.ID, stmt)
				}
				cancel()
			}()
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				if stmt.Flags&sqlgen.STMT_PREPARED > 0 {
					rs2, err2 = doStmt(ctx, c2p, round.ID, stmt)
				} else {
					rs2, err2 = doStmt(ctx, c2, round.ID, stmt)
				}
				cancel()
			}()
			wg.Wait()

			// validate result
			if err1 == sql.ErrConnDone {
				return fmt.Errorf("dsn1 %v @(%s) %q", err1, stmtID, stmt)
			}
			if err2 == sql.ErrConnDone {
				return fmt.Errorf("dsn2 %v @(%s) %q", err2, stmtID, stmt)
			}
			if !validateErrs(err1, err2) {
				return fmt.Errorf("errors mismatch: %v <> %v @(%s) %q", err1, err2, stmtID, stmt)
			}
			if rs1 == nil || rs2 == nil {
				log.Printf("skip same query error: [%v] [%v] @(%s)", err1, err2, stmtID)
				continue
			}

			digestOpts := sqlz.DigestOptions{
				Sort: stmt.Flags&sqlgen.STMT_SORTED == 0,
				Filter: func(i int, j int, raw []byte, def sqlz.ColumnDef) bool {
					if strings.Contains(stmt.Query, "union") {
						typ := rs1.ColumnDef(j).Type
						return typ != "FLOAT" && typ != "DOUBLE" && typ != "DECIMAL"
					}
					return true
				},
			}
			h1 := rs1.DataDigest(digestOpts)
			h2 := rs2.DataDigest(digestOpts)
			if h1 != h2 {
				return &ErrResultMismatch{stmtID, stmt, rs1, rs2}
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
				return fmt.Errorf("post check table %s: data mismatch", t)
			}
		}
	}

	return nil
}

func doStmt(ctx context.Context, conn sqlz.ConnContext, round string, stmt sqlgen.Stmt) (*sqlz.ResultSet, error) {
	// FIXME: prepared statement
	if stmt.Flags&sqlgen.STMT_QUERY > 0 {
		rows, err := conn.QueryContext(ctx, "/* tp-test:"+round+" */ "+stmt.Query, stmt.Params...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return sqlz.ReadFromRows(rows)
	} else {
		res, err := conn.ExecContext(ctx, "/* tp-test:"+round+" */ "+stmt.Query, stmt.Params...)
		if err != nil {
			return nil, err
		}
		return sqlz.NewFromResult(res), nil
	}
}

func validateErrs(err1 error, err2 error) bool {
	return (err1 == nil && err2 == nil) || (err1 != nil && err2 != nil)
}

func checkTables(ctx context.Context, db *sql.DB, name string) (map[string]string, error) {
	if len(name) == 0 {
		if err := db.QueryRow("select database()").Scan(&name); err != nil {
			return nil, err
		}
	}
	rows, err := db.QueryContext(ctx, "select table_name from information_schema.tables where table_schema = ?", name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	hs := make(map[string]string)
	for rows.Next() {
		var t string
		if err = rows.Scan(&t); err != nil {
			return nil, err
		}
		if hs[t], err = checkTable(ctx, db, fmt.Sprintf("`%s`.`%s`", name, t)); err != nil {
			return nil, err
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return hs, nil
}

func checkTable(ctx context.Context, db *sql.DB, name string) (string, error) {
	_, err := db.ExecContext(ctx, "admin check table "+name)
	if err != nil {
		if e, ok := err.(*mysql.MySQLError); !ok || e.Number != 1064 {
			return "", err
		}
	}
	rows, err := db.QueryContext(ctx, "select * from "+name)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	rs, err := sqlz.ReadFromRows(rows)
	if err != nil {
		return "", err
	}
	return rs.DataDigest(sqlz.DigestOptions{
		Sort: true,
		Filter: func(i int, j int, raw []byte, def sqlz.ColumnDef) bool {
			return rs.ColumnDef(j).Type != "JSON"
		},
	}), nil

}

func dumpTest(out io.Writer, opts playOptions, test TestRound, err error) {
	if out == nil {
		name := filepath.Join(opts.OutDir, test.ID+".txt")
		log.Printf("dump failure info to " + name)
		os.MkdirAll(opts.OutDir, 0755)
		f, e := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if e != nil {
			log.Printf("cannot open file: %v", e)
			return
		}
		defer f.Close()
		out = f
	}
	for _, stmt := range test.Init {
		fmt.Fprintf(out, "/* INIT */ %s\n", stmt.String())
	}
	seq := 0
	for i, test := range test.Tests {
		for _, stmt := range test {
			seq++
			fmt.Fprintf(out, "/* %02d:%03d */ %s\n", i, seq, stmt.String())
		}
	}
	if err != nil {
		fmt.Fprintln(out, "---------")
		fmt.Fprintln(out, err.Error())
	}
}

type ErrResultMismatch struct {
	id   string
	stmt sqlgen.Stmt
	rs1  *sqlz.ResultSet
	rs2  *sqlz.ResultSet
}

func (e *ErrResultMismatch) Error() string {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "result mismatch @(%s) %q\n", e.id, e.stmt.String())
	fmt.Fprintln(buf, "<<<")
	dumpResultSet(buf, e.rs1)
	fmt.Fprintln(buf, "===")
	dumpResultSet(buf, e.rs2)
	fmt.Fprintln(buf, ">>>")
	return buf.String()
}

func dumpResultSet(out io.Writer, rs *sqlz.ResultSet) {
	table := tablewriter.NewWriter(out)
	table.SetAutoFormatHeaders(false)
	table.SetAutoWrapText(false)
	rs.Dump(table)
	table.Render()
}

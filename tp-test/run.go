package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/zyguan/sqlz"
	"github.com/zyguan/sqlz/resultset"

	. "github.com/zyguan/just"
)

type connector interface {
	name() string
	get(i int) (*sql.Conn, error)
	close()
	rollback()
}

type singleConn struct {
	ctx context.Context
	c   *sql.Conn
	n   string
}

func (cc *singleConn) name() string {
	return cc.n
}

func (cc *singleConn) get(i int) (*sql.Conn, error) {
	return cc.c, nil
}

func (cc *singleConn) close() {
	cc.c.Close()
}

func (cc *singleConn) rollback() {
	cc.c.ExecContext(cc.ctx, "rollback")
}

type multiConns struct {
	ctx context.Context
	db  *sql.DB
	n   string
	m   map[int]*sql.Conn
}

func (cc *multiConns) name() string {
	return cc.n
}

func (cc *multiConns) get(i int) (*sql.Conn, error) {
	c, ok := cc.m[i]
	if !ok {
		if x, err := sqlz.Connect(cc.ctx, cc.db); err == nil {
			if _, err = x.ExecContext(cc.ctx, "use "+cc.n); err != nil {
				return nil, err
			}
			cc.m[i], c = x, x
		} else {
			return nil, err
		}
	}
	return c, nil
}

func (cc *multiConns) rollback() {
	for _, c := range cc.m {
		c.Close()
	}
}

func (cc *multiConns) close() {
	for _, c := range cc.m {
		c.ExecContext(cc.ctx, "rollback")
	}
}

type runABTestOptions struct {
	Threads      int
	QueryTimeout int
	Continue     func() bool

	Tag1 string
	Tag2 string
	DB1  *sql.DB
	DB2  *sql.DB

	Store Store
}

func runABTest(ctx context.Context, failed chan struct{}, opts runABTestOptions) error {
	store := opts.Store

	if opts.Continue == nil {
		opts.Continue = func() bool { return true }
	}
	for opts.Continue() {
		select {
		case <-failed:
			return nil
		case <-ctx.Done():
			return nil
		default:
		}

		t, err := store.NextPendingTest()
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				log.Printf("no more test")
				select {
				case <-failed:
					return nil
				case <-ctx.Done():
					return nil
				case <-time.After(time.Duration(rand.Intn(10*opts.Threads)) * time.Second):
					continue
				}
			}
			return err
		}

		// using different db names allows us run test on same db instance
		dbName1 := "db1__" + strings.ReplaceAll(t.ID, "-", "_")
		dbName2 := "db2__" + strings.ReplaceAll(t.ID, "-", "_")

		c1, err1 := initTest(ctx, opts.DB1, dbName1, t)
		if err1 != nil {
			store.SetTest(t.ID, TestFailed, "init db1: "+err1.Error())
			log.Printf("failed to init %s/%s: %v", opts.Tag1, dbName1, err1)
			continue
		}
		c2, err2 := initTest(ctx, opts.DB2, dbName2, t)
		if err2 != nil {
			c1.close()
			store.SetTest(t.ID, TestFailed, "init db2: "+err2.Error())
			log.Printf("failed to init %s/%s: %v", opts.Tag2, dbName2, err2)
			continue
		}

		log.Printf("run test %s %s", t.ID, t.Mode)
		switch t.Mode {
		case ModeDefault, ModeSequence:
			if err := runSequence(ctx, failed, opts, t, c1, c2); err != nil {
				return err
			}
		case ModeMultiSession:
			if err := runMultiSession(ctx, failed, opts, t, c1, c2); err != nil {
				return err
			}
		}
		c1.close()
		c2.close()

		store.SetTest(t.ID, TestPassed, "")

		opts.DB1.ExecContext(ctx, "drop database if exists "+dbName1)
		opts.DB2.ExecContext(ctx, "drop database if exists "+dbName2)
	}
	return nil
}

func initTest(ctx context.Context, db *sql.DB, name string, t *Test) (c connector, err error) {
	defer Return(&err)
	conn := Try(sqlz.Connect(ctx, db)).(*sql.Conn)
	Try(conn.ExecContext(ctx, "create database "+name))
	Try(conn.ExecContext(ctx, "use "+name))
	for _, stmt := range t.InitSQL {
		// it's ok for some of stmts in init_sql failed
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			log.Printf("init stmt failed: %v @ %s", err, stmt)
		}
	}
	switch t.Mode {
	case ModeDefault, ModeSequence:
		return &singleConn{
			ctx: ctx,
			c:   conn,
			n:   name,
		}, nil
	case ModeMultiSession:
		return &multiConns{
			ctx: ctx,
			db:  db,
			n:   name,
			m:   map[int]*sql.Conn{0: conn},
		}, nil
	default:
		return nil, errors.New("unknown test mode: " + t.Mode)
	}
}

func runSequence(ctx context.Context, failed chan struct{}, opts runABTestOptions, t *Test, c1 connector, c2 connector) error {
	for i := range t.Groups {
		fail := func(err error) error {
			defer func() { recover() }()
			c1.rollback()
			c2.rollback()
			c1.close()
			c2.close()
			opts.Store.SetTest(t.ID, TestFailed, err.Error())
			log.Printf("test(%s) failed at txn #%d: %v", t.ID, i, err)
			close(failed)
			return err
		}

		if err := doStmts(ctx, opts, t.ID, t.Groups[i], c1, c2); err != nil {
			return fail(err)
		}

		hs1, err1 := checkTables(ctx, opts.DB1, c1.name())
		if err1 != nil {
			return fail(fmt.Errorf("check table of %s after txn #%d: %v", opts.Tag1, i, err1))
		}
		hs2, err2 := checkTables(ctx, opts.DB1, c2.name())
		if err2 != nil {
			return fail(fmt.Errorf("check table of %s after txn #%d: %v", opts.Tag2, i, err2))
		}
		for t := range hs2 {
			if hs1[t] != hs2[t] {
				return fail(fmt.Errorf("data mismatch after txn #%d: %s != %s @%s", i, hs1[t], hs2[t], t))
			}
		}
	}
	return nil
}

func runMultiSession(ctx context.Context, failed chan struct{}, opts runABTestOptions, t *Test, c1 connector, c2 connector) error {
	fail := func(err error) error {
		defer func() { recover() }()
		c1.rollback()
		c2.rollback()
		c1.close()
		c2.close()
		opts.Store.SetTest(t.ID, TestFailed, err.Error())
		log.Printf("test(%s) failed: %v", t.ID, err)
		close(failed)
		return err
	}
	if err := doStmts(ctx, opts, t.ID, t.OrderedStmts(), c1, c2); err != nil {
		return fail(err)
	}
	hs1, err1 := checkTables(ctx, opts.DB1, c1.name())
	if err1 != nil {
		return fail(fmt.Errorf("check table of %s: %v", opts.Tag1, err1))
	}
	hs2, err2 := checkTables(ctx, opts.DB1, c2.name())
	if err2 != nil {
		return fail(fmt.Errorf("check table of %s: %v", opts.Tag2, err2))
	}
	for t := range hs2 {
		if hs1[t] != hs2[t] {
			return fail(fmt.Errorf("data mismatch: %s != %s @%s", hs1[t], hs2[t], t))
		}
	}
	return nil
}

func checkErrs(err1 error, err2 error) bool {
	return (err1 == nil && err2 == nil) || (err1 != nil && err2 != nil)
}

func checkTables(ctx context.Context, db *sql.DB, name string) (map[string]string, error) {
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
	rs, err := resultset.ReadFromRows(rows)
	if err != nil {
		return "", err
	}
	return unorderedDigest(rs, func(col resultset.ColumnDef) bool {
		return col.Type != "JSON"
	}), nil
}

func doStmts(ctx context.Context, opts runABTestOptions, id string, stmts StmtList, c1 connector, c2 connector) error {
	record := func(seq int, tag string, rs *resultset.ResultSet, err error) {
		if rs != nil {
			raw, _ := rs.Encode()
			opts.Store.PutStmtResult(id, seq, tag, Result{
				Raw:          raw,
				Err:          err,
				RowsAffected: rs.ExecResult().RowsAffected,
				LastInsertId: rs.ExecResult().LastInsertId,
			})
		} else {
			opts.Store.PutStmtResult(id, seq, tag, Result{Err: err})
		}
	}

	for _, stmt := range stmts {
		ctx1, _ := context.WithTimeout(ctx, time.Duration(opts.QueryTimeout)*time.Second)
		rs1, err1 := doStmt(ctx1, c1, stmt)
		record(stmt.Seq, opts.Tag1, rs1, err1)
		ctx2, _ := context.WithTimeout(ctx, time.Duration(opts.QueryTimeout)*time.Second)
		rs2, err2 := doStmt(ctx2, c2, stmt)
		record(stmt.Seq, opts.Tag2, rs2, err2)
		if !checkErrs(err1, err2) {
			return fmt.Errorf("errors mismatch: %v <> %v @(%s,%d) %q", err1, err2, id, stmt.Seq, stmt.Stmt)
		}
		if rs1 == nil || rs2 == nil {
			log.Printf("skip query error: [%v] [%v] @(%s,%d)", err1, err2, id, stmt.Seq)
			continue
		}
		h1, h2 := "", ""
		if q := strings.ToLower(stmt.Stmt); stmt.IsQuery && rs1.NRows() == rs2.NRows() && rs1.NRows() > 1 &&
			(!strings.Contains(q, "order by") || strings.Contains(q, "force-unordered")) {
			h1, h2 = unorderedDigest(rs1, nil), unorderedDigest(rs2, nil)
		} else {
			h1, h2 = rs1.DataDigest(), rs2.DataDigest()
		}
		if h1 != h2 {
			return fmt.Errorf("result digests mismatch: %s != %s @(%s,%d) %q", h1, h2, id, stmt.Seq, stmt.Stmt)
		}
		if rs1.IsExecResult() && rs1.ExecResult().RowsAffected != rs2.ExecResult().RowsAffected {
			return fmt.Errorf("rows affected mismatch: %d != %d @(%s,%d) %q",
				rs1.ExecResult().RowsAffected, rs2.ExecResult().RowsAffected, id, stmt.Seq, stmt.Stmt)
		}
	}
	return nil
}

func doStmt(ctx context.Context, c connector, stmt Stmt) (*resultset.ResultSet, error) {
	s, err := c.get(stmt.Txn)
	if err != nil {
		return nil, err
	}
	if stmt.IsQuery {
		rows, err := s.QueryContext(ctx, stmt.Stmt)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return resultset.ReadFromRows(rows)
	} else {
		res, err := s.ExecContext(ctx, stmt.Stmt)
		if err != nil {
			return nil, err
		}
		return resultset.NewFromResult(res), nil
	}
}

func unorderedDigest(rs *resultset.ResultSet, colFilter func(resultset.ColumnDef) bool) string {
	if colFilter == nil {
		colFilter = func(_ resultset.ColumnDef) bool { return true }
	}
	cols := make([]int, 0, rs.NCols())
	for i := 0; i < rs.NCols(); i++ {
		if colFilter(rs.ColumnDef(i)) {
			cols = append(cols, i)
		}
	}
	digests := make(rows, rs.NRows())
	for i := 0; i < rs.NRows(); i++ {
		h := sha1.New()
		for _, j := range cols {
			raw, _ := rs.RawValue(i, j)
			h.Write(raw)
		}
		digests[i] = h.Sum(nil)
	}
	sort.Sort(digests)
	h := sha1.New()
	for _, digest := range digests {
		h.Write(digest)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type rows [][]byte

func (r rows) Len() int { return len(r) }

func (r rows) Less(i, j int) bool { return bytes.Compare(r[i], r[j]) < 0 }

func (r rows) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

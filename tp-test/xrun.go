package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/zyguan/sqlz"
	"github.com/zyguan/sqlz/resultset"
)

type XStmtResult struct {
	Stmt
	Error      error
	Result     *resultset.ResultSet
	StartedAt  time.Time
	FinishedAt time.Time
}

func (r *XStmtResult) Digest() string {
	if q := strings.ToLower(r.Stmt.Stmt); r.Result.NRows() > 1 &&
		(!strings.Contains(q, "order by") || strings.Contains(q, "force-unordered")) {
		return unorderedDigest(r.Result, nil)
	}
	return r.Result.DataDigest()
}

func (r *XStmtResult) String() string {
	if r.Error != nil {
		return fmt.Sprintf("Err(%v)", r.Error)
	}
	if r.Result.IsExecResult() {
		return fmt.Sprintf("Ok(rows_affected:%d)", r.Result.ExecResult().RowsAffected)
	}
	return fmt.Sprintf("Ok(rows:%d,digest:%s)", r.Result.NRows(), r.Digest())
}

type XStmtStream struct {
	conn *sql.Conn
	ctx  context.Context
	lst  []*Stmt
	idx  int
	wip  chan struct{}
}

func (s *XStmtStream) Step(seq int, cb func(res XStmtResult)) {
	stmt := s.lst[s.idx]
	stmt.Seq = seq

	s.wip = make(chan struct{})
	go func() {
		defer close(s.wip)
		res := XStmtResult{
			Stmt:      *stmt,
			StartedAt: time.Now(),
		}
		onErr := func(err error) {
			res.Error = err
			res.FinishedAt = time.Now()
			if cb != nil {
				cb(res)
			}
		}
		if stmt.IsQuery {
			rows, err := s.conn.QueryContext(s.ctx, stmt.Stmt)
			if err != nil {
				onErr(err)
				return
			}
			defer rows.Close()
			res.Result, res.Error = resultset.ReadFromRows(rows)
		} else {
			r, err := s.conn.ExecContext(s.ctx, stmt.Stmt)
			if err != nil {
				onErr(err)
				return
			}
			res.Result = resultset.NewFromResult(r)
		}
		res.FinishedAt = time.Now()
		if cb != nil {
			cb(res)
		}
	}()

	select {
	case <-s.wip:
		s.wip = nil
	case <-time.After(5 * time.Second):
	}
	s.idx += 1

}

func (s *XStmtStream) Blocking() bool {
	if s.wip == nil {
		return false
	}
	select {
	case <-s.wip:
		s.wip = nil
		return false
	default:
		return true
	}
}

func (s *XStmtStream) Wait() {
	if s.wip == nil {
		return
	}
	<-s.wip
	s.wip = nil
}

func (s *XStmtStream) Head() Stmt {
	return *s.lst[s.idx]
}

func (s *XStmtStream) HasNext() bool {
	return s.idx < len(s.lst)
}

func (s *XStmtStream) Close() error {
	return s.conn.Close()
}

func XRun(ctx context.Context, store Store, db *sql.DB, t Test) (err error) {
	t.ID = uuid.New().String()
	t.Status = TestRunning
	t.StartedAt = time.Now().Unix()
	t, err = store.SetXTest(t)
	if err != nil {
		return err
	}

	fail := func(err error) error {
		t.Status = TestFailed
		t.FinishedAt = time.Now().Unix()
		t.Message = err.Error()
		store.SetXTest(t)
		return err
	}

	n1 := "db1__" + strings.ReplaceAll(t.ID, "-", "_")
	n2 := "db2__" + strings.ReplaceAll(t.ID, "-", "_")

	rs, err := xRunA(ctx, store, db, n1, t)
	if err != nil {
		return fail(err)
	}

	err = xRunB(ctx, store, db, n2, t, rs)
	if err != nil {
		return fail(err)
	}

	hs1, err1 := checkTables(ctx, db, n1)
	if err1 != nil {
		return fail(fmt.Errorf("check tables of %s: %v", n1, err1))
	}
	hs2, err2 := checkTables(ctx, db, n2)
	if err2 != nil {
		return fail(fmt.Errorf("check tables of %s: %v", n2, err2))
	}
	for t := range hs2 {
		if hs1[t] != hs2[t] {
			return fail(fmt.Errorf("data mismatch: %s != %s @%s", hs1[t], hs2[t], t))
		}
	}

	t.Status = TestPassed
	t.FinishedAt = time.Now().Unix()
	store.SetXTest(t)

	db.Exec("drop database " + n1)
	db.Exec("drop database " + n2)

	return nil
}

func xRunA(ctx context.Context, store Store, db *sql.DB, n string, t Test) ([]*XStmtResult, error) {
	conn, err := sqlz.Connect(ctx, db)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "create database "+n)
	if err != nil {
		return nil, err
	}
	_, err = conn.ExecContext(ctx, "use "+n)
	if err != nil {
		return nil, err
	}

	for _, stmt := range t.InitSQL {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			log.Printf("failed to execute init stmt: %v @ %s", err, stmt)
		}
	}

	size, ss := 0, make([]*XStmtStream, len(t.Groups))
	defer func() {
		for _, s := range ss {
			if s != nil {
				s.Close()
			}
		}
	}()

	for i, lst := range t.Groups {
		size += len(lst)
		c, err := sqlz.Connect(ctx, db)
		if err != nil {
			return nil, err
		}
		if _, err := c.ExecContext(ctx, "use "+n); err != nil {
			return nil, err
		}
		ss[i] = &XStmtStream{
			conn: c,
			ctx:  ctx,
			lst:  make([]*Stmt, len(lst)),
		}
		for j := range lst {
			ss[i].lst[j] = &lst[j]
		}
	}

	rs := make([]*XStmtResult, size)
	alts := make([]*XStmtStream, 0, len(t.Groups))
	trials, seq := 0, 0
	for seq < size {
		alts = alts[:0]
		for _, s := range ss {
			if s.HasNext() && !s.Blocking() {
				alts = append(alts, s)
			}
		}
		if len(alts) == 0 {
			if trials < 5 {
				time.Sleep(time.Duration(1+trials*2) * time.Second)
				trials += 1
				continue
			}
			return nil, errors.New("all transactions may be blocked")
		}
		trials = 0
		s := alts[rand.Intn(len(alts))]
		s.Step(seq, func(res XStmtResult) {
			rs[res.Seq] = &res
			if err := store.PutXStmtResult(t.ID, 0, res); err != nil {
				log.Printf("failed to write x_stmt_0: %v", err)
			}
		})
		seq += 1
	}

	for _, s := range ss {
		s.Wait()
	}

	return rs, nil
}

func xRunB(ctx context.Context, store Store, db *sql.DB, n string, t Test, rs []*XStmtResult) error {
	conn, err := sqlz.Connect(ctx, db)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "create database "+n)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "use "+n)
	if err != nil {
		return err
	}

	for _, stmt := range t.InitSQL {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			log.Printf("failed to execute init stmt: %v @ %s", err, stmt)
		}
	}

	sort.Slice(t.Groups, func(i, j int) bool {
		t1, t2 := t.Groups[i], t.Groups[j]
		return t1[len(t1)-1].Seq < t2[len(t2)-1].Seq
	})
	for _, lst := range t.Groups {
		for _, stmt := range lst {
			r1 := rs[stmt.Seq]
			if r1.Error != nil && strings.Contains(strings.ToLower(r1.Error.Error()), "lock") {
				log.Printf("skip execute %q due to %v", stmt.Stmt, r1.Error.Error())
				continue
			}
			r2 := XStmtResult{
				Stmt:      stmt,
				StartedAt: time.Now(),
			}
			if stmt.IsQuery {
				rows, err := conn.QueryContext(ctx, stmt.Stmt)
				if err != nil {
					r2.Error = err
				} else {
					r2.Result, r2.Error = resultset.ReadFromRows(rows)
					rows.Close()
				}
			} else {
				r, err := conn.ExecContext(ctx, stmt.Stmt)
				if err != nil {
					r2.Error = err
				} else {
					r2.Result = resultset.NewFromResult(r)
				}
			}
			r2.FinishedAt = time.Now()
			if err := store.PutXStmtResult(t.ID, 1, r2); err != nil {
				log.Printf("failed to write x_stmt_1: %v", err)
			}
			if !checkErrs(r1.Error, r2.Error) {
				return fmt.Errorf("errors mismatch: %v <> %v @(%s,%d) %q", r1.Error, r2.Error, t.ID, stmt.Seq, stmt.Stmt)
			}
			if r1.Result == nil || r2.Result == nil {
				log.Printf("skip query error: [%v] [%v] @(%s,%d)", r1.Error, r2.Error, t.ID, stmt.Seq)
				continue
			}
			if h1, h2 := r1.Digest(), r2.Digest(); h1 != h2 {
				return fmt.Errorf("result digests mismatch: %s != %s @(%s,%d) %q", h1, h2, t.ID, stmt.Seq, stmt.Stmt)
			}
			if r1.Result.IsExecResult() && r1.Result.ExecResult().RowsAffected != r2.Result.ExecResult().RowsAffected {
				return fmt.Errorf("rows affected mismatch: %d != %d @(%s,%d) %q",
					r1.Result.ExecResult().RowsAffected, r2.Result.ExecResult().RowsAffected,
					t.ID, stmt.Seq, stmt.Stmt)
			}
		}
	}
	return nil
}

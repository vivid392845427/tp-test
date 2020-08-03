package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/zyguan/sqlz"
	"github.com/zyguan/xs"

	. "github.com/zyguan/just"
)

const (
	TestPending = "Pending"
	TestRunning = "Running"
	TestFailed  = "Failed"
	TestPassed  = "Passed"
	TestUnknown = "Unknown"
)

type Store interface {
	Init() error
	Clear() error
	LoadInits() ([]Init, error)
	AddTest(test Test) error
	NextPendingTest() (*Test, error)
	SetTest(id string, status string, message string) error
	PutStmtResult(id string, seq int, tag string, result []byte, err error) error
}

type Init struct {
	ID      int
	InitSQL string
}

type Test struct {
	ID         string
	Status     string
	Message    string
	StartedAt  int64
	FinishedAt int64
	Init       *Init
	Steps      []Txn
}

type Txn []Stmt

type Stmt struct {
	Seq     int
	Txn     int
	TestID  string
	Stmt    string
	IsQuery bool
}

func NewStore(dsn string) (Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &store{db: sqlz.WrapDB(context.Background(), db)}, nil
}

type store struct {
	db *sqlz.DB
}

func (s *store) Init() error { return initDB(s.db) }

func (s *store) Clear() error { return clearDB(s.db) }

func (s *store) LoadInits() (inits []Init, err error) {
	defer Return(&err)
	rows := s.db.MustQuery("select id, init_sql from init where enabled = true")
	defer rows.Close()
	for rows.Next() {
		var init Init
		Try(rows.Scan(&init.ID, &init.InitSQL))
		inits = append(inits, init)
	}
	Try(rows.Err())
	return inits, nil
}

func (s *store) AddTest(test Test) (err error) {
	defer Return(&err)
	if test.Init == nil {
		return errors.New("init is missing")
	}
	if len(test.ID) == 0 {
		test.ID = uuid.New().String()
	}

	tx := Try(s.db.Begin()).(*sql.Tx)
	defer tx.Rollback()

	seq := 0
	for i, txn := range test.Steps {
		for _, stmt := range txn {
			Try(tx.Exec("insert into stmt (test_id, seq, txn, stmt, is_query) values (?, ?, ?, ?, ?)",
				test.ID, seq, i, stmt.Stmt, naiveQueryDetect(stmt.Stmt)))
			seq += 1
		}
	}
	Try(tx.Exec("insert into test (id, init_id, status) values (?, ?, ?)", test.ID, test.Init.ID, TestPending))

	return tx.Commit()
}

func (s *store) NextPendingTest() (test *Test, err error) {
	defer Return(&err)
	var (
		x Init
		t = Test{
			Status:    TestPending,
			StartedAt: time.Now().Unix(),
		}
		stmts []Stmt
	)

	t.StartedAt = time.Now().Unix()

	tx := Try(s.db.Begin()).(*sql.Tx)
	defer tx.Rollback()

	Try(tx.QueryRow("select id, init_id from test where status = ? limit 1 for update", t.Status).Scan(&t.ID, &x.ID))
	Try(tx.QueryRow("select init_sql from init where id = ?", x.ID).Scan(&x.InitSQL))
	Try(tx.Exec("update test set status = ?, started_at = ? where id = ?", TestRunning, t.StartedAt, t.ID))

	rows := Try(tx.Query("select txn, seq, stmt, is_query from stmt where test_id = ? order by txn, seq", t.ID)).(*sql.Rows)
	defer rows.Close()
	for rows.Next() {
		var stmt Stmt
		Try(rows.Scan(&stmt.Txn, &stmt.Seq, &stmt.Stmt, &stmt.IsQuery))
		stmt.TestID = t.ID
		stmts = append(stmts, stmt)
	}
	Try(rows.Err())
	Try(tx.Commit())

	t.Init = &x
	for i := 0; i < len(stmts); {
		j := i + 1
		for ; j < len(stmts); j++ {
			if stmts[i].Txn != stmts[j].Txn {
				break
			}
		}
		t.Steps = append(t.Steps, stmts[i:j])
		i = j
	}

	return &t, nil
}

func (s *store) SetTest(id string, status string, message string) error {
	_, err := s.db.Exec("update test set status = ?, message = ?, finished_at = ? where id = ?",
		status, message, time.Now().Unix(), id)
	return err
}

func (s *store) PutStmtResult(id string, seq int, tag string, result []byte, error error) error {
	errmsg := ""
	if error != nil {
		errmsg = error.Error()
	}
	_, err := s.db.Exec("insert into stmt_result (test_id, seq, tag, error, result, created_at) values (?, ?, ?, ?, ?, ?)",
		id, seq, tag, errmsg, result, time.Now().Unix())
	return err
}

func initDB(db *sqlz.DB) (err error) {
	defer Return(&err)

	db.MustExec(`create table test (
    id char(36) not null,
    init_id int not null,
    status varchar(20),
    started_at bigint,
    finished_at bigint,
    message text,
    primary key (id),
    key (status)
)`)
	db.MustExec(`create table stmt (
    test_id char(36) not null,
    seq int not null,
    txn int not null,
    stmt text not null,
    is_query bool,
    primary key (test_id, seq)
)`)
	db.MustExec(`create table stmt_result (
    id bigint not null auto_increment,
    test_id char(36) not null,
    seq int not null,
    tag varchar(40) not null,
    error text,
    result longblob,
    created_at int not null,
    primary key (id),
    key (test_id, seq)
)`)
	db.MustExec(`create table init (
    id int not null auto_increment,
    init_sql longtext not null,
    enabled bool default true,
    primary key (id)
)`)

	buf := new(bytes.Buffer)
	rand.Seed(0)
	xs.Walk(schemaRule(), func(ss []string) {
		buf.Reset()
		buf.WriteString(strings.Join(ss, " "))
		buf.WriteByte(';')
		insertData(buf)
		db.MustExec("insert into init (init_sql) values (?)", buf.String())
	})

	return nil
}

func clearDB(db *sqlz.DB) error {
	_, err := db.Exec("drop table if exists test, init, stmt, stmt_result")
	return err
}

func insertData(buf *bytes.Buffer) {
	vals := make([]string, 16)
	for i := range vals {
		vals[i] = fmt.Sprintf("(%d,%.6f,%.3f,'%s','%s','%s','%s','%s','%s')",
			i+1, randDouble(), randDecimal(), randString(),
			randDatetime(), randTimestamp(), randEnum(), randSet(), randJson())
	}
	buf.WriteString("insert into t values ")
	buf.WriteString(strings.Join(vals, ", "))
	buf.WriteByte(';')
}

func naiveQueryDetect(sql string) bool {
	sql = strings.ToLower(strings.TrimSpace(sql))
	for _, w := range []string{"select ", "show ", "admin show "} {
		if strings.HasPrefix(sql, w) {
			return true
		}
	}
	return false
}

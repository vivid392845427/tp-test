package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/zyguan/sqlz"
	"github.com/zyguan/sqlz/resultset"

	_ "github.com/go-sql-driver/mysql"

	. "github.com/zyguan/just"
)

const (
	modeEval = "eval"
	modeRand = "rand"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type options struct {
	dsn     string
	init    string
	mode    string
	input   string
	verbose bool

	blockTime time.Duration
}

func main() {
	defer Catch(func(c Catchable) {
		fmt.Fprintf(os.Stderr, "\x1b[0;31mError: %+v\x1b[0m\n", c.Why())
	})
	var opts options
	flag.StringVar(&opts.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "data source name")
	flag.StringVar(&opts.init, "init", "INIT", "init session name")
	flag.StringVar(&opts.mode, "m", modeEval, "run mode (eval, rand)")
	flag.StringVar(&opts.input, "i", "", "input file")
	flag.BoolVar(&opts.verbose, "v", false, "verbose (show select results)")
	flag.DurationVar(&opts.blockTime, "block-time", 2*time.Second, "max wait time to run a stmt synchronously")

	flag.Parse()

	db := Try(sql.Open("mysql", opts.dsn)).(*sql.DB)
	defer db.Close()
	ex := newExecutor(context.Background(), db)
	defer ex.Shutdown()

	f := os.Stdin
	if len(opts.input) > 0 {
		f = Try(os.Open(opts.input)).(*os.File)
		defer f.Close()
	}

	switch opts.mode {
	case modeEval:
		mustEval(ex, readInput(f), opts)
	case modeRand:
		mustRand(ex, readInput(f), opts)
	default:
		Throw("unknown mode: " + opts.mode)
	}
}

func readInput(r io.Reader) [][3]string {
	lst := make([][3]string, 0, 16)
	in := bufio.NewScanner(r)
	in.Split(bufio.ScanLines)
	for in.Scan() {
		line := in.Text()
		sess, stmt := split(line)
		if len(sess) == 0 {
			continue
		}
		lst = append(lst, [3]string{line, sess, stmt})
	}
	return lst
}

func rdump(s string, v bool) func(r *resultset.ResultSet, e error) {
	return func(r *resultset.ResultSet, e error) {
		if e != nil {
			fmt.Println("--", s, e.Error())
		} else {
			if v && !r.IsExecResult() {
				buf := new(bytes.Buffer)
				r.PrettyPrint(buf)
				for {
					line, err := buf.ReadString('\n')
					if err != nil {
						break
					}
					fmt.Print("-- ", s, " ", line)
				}
			}
			fmt.Println("--", s, r.String())
		}
	}
}

func mustEval(ex *Executor, ts [][3]string, opts options) {
	type node struct {
		t    [3]string
		next *node
	}
	head := new(node)
	for i := 0; i < len(ts); i++ {
		head.next = &node{ts[len(ts)-i-1], head.next}
	}
	for head.next != nil {
		blocked := map[string]bool{}
		for p := head; p.next != nil; p = p.next {
			line, sess, stmt := p.next.t[0], p.next.t[1], p.next.t[2]
			if blocked[sess] {
				continue
			}
			status := Try(ex.Execute(sess, func() string {
				fmt.Println(line)
				return stmt
			}, ExecOptions{
				WaitBefore: 100 * time.Millisecond,
				WaitAfter:  opts.blockTime,
				Callback:   rdump(sess, opts.verbose),
			})).(ExecStatus)
			if status == ExecBlocked {
				blocked[sess] = true
				continue
			}
			if status == ExecRunning {
				fmt.Println("--", sess, "is blocked")
			}
			p.next = p.next.next
			break
		}
	}
}

func mustRand(ex *Executor, ts [][3]string, opts options) {
	qs := make(map[string][][3]string)
	for _, t := range ts {
		line, sess, stmt := t[0], t[1], t[2]
		if sess != opts.init {
			q := qs[sess]
			q = append(q, t)
			qs[sess] = q
			continue
		}
		Try(ex.Execute(sess, func() string {
			fmt.Println(line)
			return stmt
		}, ExecOptions{Callback: rdump(sess, opts.verbose)}))
	}
	var lock sync.Mutex
	choose := func() string {
		lock.Lock()
		defer lock.Unlock()
		ns := make([]string, 0, len(qs))
		for n := range qs {
			ns = append(ns, n)
		}
		return ns[rand.Intn(len(ns))]
	}
	for len(qs) > 0 {
		s := choose()
		status := Try(ex.Execute(s, func() string {
			lock.Lock()
			defer lock.Unlock()
			q := qs[s]
			if len(q) == 1 {
				delete(qs, s)
			} else {
				qs[s] = q[1:]
			}
			line, stmt := q[0][0], q[0][2]
			fmt.Println(line)
			return stmt
		}, ExecOptions{
			WaitBefore: 100 * time.Millisecond,
			WaitAfter:  opts.blockTime,
			Callback:   rdump(s, opts.verbose),
		})).(ExecStatus)
		if status == ExecRunning {
			fmt.Println("--", s, "is blocked")
		}
	}
}

var re = regexp.MustCompile(`^/\*\s*(\w+)\s*\*/\s+(.*);.*$`)

func split(line string) (string, string) {
	ss := re.FindStringSubmatch(line)
	if len(ss) != 3 {
		return "", ""
	}
	return ss[1], ss[2]
}

type Executor struct {
	ctx context.Context
	db  *sql.DB
	m   map[string]*sql.Conn
	x   map[string]chan struct{}
}

func newExecutor(ctx context.Context, db *sql.DB) *Executor {
	return &Executor{
		ctx: ctx,
		db:  db,
		m:   map[string]*sql.Conn{},
		x:   map[string]chan struct{}{},
	}
}

type ExecStatus string

const (
	ExecDone    = "Done"
	ExecFailed  = "Failed"
	ExecRunning = "Running"
	ExecBlocked = "Blocked"
)

type ExecOptions struct {
	WaitBefore time.Duration
	WaitAfter  time.Duration
	Callback   func(r *resultset.ResultSet, e error)
}

func (ex *Executor) Execute(s string, invoke func() string, opts ExecOptions) (ExecStatus, error) {
	c, ok := ex.m[s]
	if !ok {
		if x, err := sqlz.Connect(ex.ctx, ex.db); err == nil {
			ex.m[s], c = x, x
		} else {
			return ExecFailed, err
		}
	}
	if f, ok := ex.x[s]; ok {
		if opts.WaitBefore > 0 {
			select {
			case <-f:
				delete(ex.x, s)
			case <-time.After(opts.WaitBefore):
				return ExecBlocked, nil
			}
		} else {
			<-f
			delete(ex.x, s)
		}
	}

	cb := func(r *resultset.ResultSet, e error) {
		if opts.Callback != nil {
			opts.Callback(r, e)
		}
	}

	ex.x[s] = make(chan struct{})
	go func() {
		defer close(ex.x[s])
		q := invoke()
		if isQuery(q) {
			rows, err := c.QueryContext(ex.ctx, q)
			if err != nil {
				cb(nil, err)
				return
			}
			defer rows.Close()
			cb(resultset.ReadFromRows(rows))
		} else {
			res, err := c.ExecContext(ex.ctx, q)
			if err != nil {
				cb(nil, err)
				return
			}
			cb(resultset.NewFromResult(res), nil)
		}
	}()

	if opts.WaitAfter <= 0 {
		<-ex.x[s]
		delete(ex.x, s)
		return ExecDone, nil
	}

	select {
	case <-ex.x[s]:
		delete(ex.x, s)
		return ExecDone, nil
	case <-time.After(opts.WaitAfter):
		return ExecRunning, nil
	}
}

func (ex *Executor) Shutdown() {
	for n, c := range ex.m {
		if ch, ok := ex.x[n]; ok {
			<-ch
			delete(ex.x, n)
		}
		c.Close()
	}
}

func isQuery(sql string) bool {
	sql = strings.ToLower(strings.TrimLeft(strings.TrimSpace(sql), "("))
	for _, w := range []string{"select ", "show ", "admin show ", "explain ", "desc ", "describe "} {
		if strings.HasPrefix(sql, w) {
			return true
		}
	}
	return false
}

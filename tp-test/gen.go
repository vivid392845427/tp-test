package main

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/go-randgen/grammar/sqlgen"
	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
)

type StmtList []sqlgen.Stmt

type TestRound struct {
	ID    string
	Init  StmtList
	Tests []StmtList
}

type genTestOptions struct {
	Grammar    string
	InitRoot   string
	TestRoot   string
	RecurLimit int
	Tests      int
	Debug      bool
}

func genTest(opts genTestOptions) (test TestRound, err error) {
	rand.Seed(time.Now().UnixNano())
	test.ID = strconv.FormatInt(time.Now().Unix(), 10) + "." + uuid.New().String()

	gen, err := sqlgen.NewGenerator(opts.Grammar, nil, setup)
	if err != nil {
		return TestRound{}, err
	}
	gen.SetRecurLimit(opts.RecurLimit).SetDebug(opts.Debug)

	lst, err := gen.SetRoot(opts.InitRoot).Generate()
	if err != nil {
		return TestRound{}, err
	}
	test.Init = lst

	gen.SetRoot(opts.TestRoot)
	for i := 0; i < opts.Tests; i++ {
		lst, err = gen.Generate()
		if err != nil {
			return TestRound{}, err
		}
		test.Tests = append(test.Tests, lst)
	}
	return
}

func setup(L *lua.LState, out io.Writer) error {
	global := make(map[string]lua.LValue)
	val := func(L *lua.LState, v lua.LValue) int {
		if v == nil {
			L.Push(lua.LNil)
		} else {
			L.Push(v)
		}
		return 1
	}
	L.SetGlobal("set", L.NewFunction(func(L *lua.LState) int {
		k := L.CheckString(1)
		v := L.CheckAny(2)
		old := global[k]
		global[k] = v
		return val(L, old)
	}))
	L.SetGlobal("del", L.NewFunction(func(L *lua.LState) int {
		k := L.CheckString(1)
		old := global[k]
		delete(global, k)
		return val(L, old)
	}))
	L.SetGlobal("get", L.NewFunction(func(L *lua.LState) int {
		k := L.CheckString(1)
		v, ok := global[k]
		if !ok && L.GetTop() > 1 {
			v = L.CheckAny(2)
		}
		return val(L, v)
	}))
	L.SetGlobal("exists", L.NewFunction(func(L *lua.LState) int {
		k := L.CheckString(1)
		_, ok := global[k]
		return val(L, lua.LBool(ok))
	}))

	L.SetGlobal("print", L.NewFunction(func(L *lua.LState) int {
		top := L.GetTop()
		for i := 1; i <= top; i++ {
			fmt.Fprint(out, L.ToStringMeta(L.Get(i)).String())
			if i != top {
				fmt.Fprint(out, "\t")
			}
		}
		return 0
	}))
	L.SetGlobal("printf", L.NewFunction(func(L *lua.LState) int {
		format := L.CheckString(1)
		args := make([]interface{}, L.GetTop()-1)
		top := L.GetTop()
		for i := 2; i <= top; i++ {
			args[i-2] = L.Get(i)
		}
		k := strings.Count(format, "%") - strings.Count(format, "%%")
		if len(args) < k {
			k = len(args)
		}
		fmt.Fprintf(out, format, args[:k]...)
		return 0
	}))
	L.SetGlobal("sprintf", L.NewFunction(func(L *lua.LState) int {
		format := L.CheckString(1)
		args := make([]interface{}, L.GetTop()-1)
		top := L.GetTop()
		for i := 2; i <= top; i++ {
			args[i-2] = L.Get(i)
		}
		k := strings.Count(format, "%") - strings.Count(format, "%%")
		if len(args) < k {
			k = len(args)
		}
		L.Push(lua.LString(fmt.Sprintf(format, args[:k]...)))
		return 1
	}))
	L.SetGlobal("timef", L.NewFunction(func(L *lua.LState) int {
		t := time.Now()
		if L.GetTop() > 0 {
			t = time.Unix(L.CheckInt64(1), 0)
		}
		format := "2006-01-02 15:04:05"
		if L.GetTop() > 1 {
			format = L.CheckString(2)
		}
		L.Push(lua.LString(t.UTC().Format(format)))
		return 1
	}))
	L.SetGlobal("random_name", L.NewFunction(func(L *lua.LState) int {
		n := adjectives[rand.Intn(len(adjectives))] + " " + surnames[rand.Intn(len(surnames))]
		L.Push(lua.LString(n))
		return 1
	}))
	return preloadLib(L, "util")
}

//go:embed lualib/*
var lualib embed.FS

func preloadLib(L *lua.LState, name string) error {
	src, err := lualib.ReadFile("lualib/" + name + ".lua")
	if err != nil {
		return err
	}
	preload := L.GetField(L.GetField(L.Get(lua.EnvironIndex), "package"), "preload")
	if _, ok := preload.(*lua.LTable); !ok {
		return errors.New("package.preload must be a table")
	}
	chunk, err := parse.Parse(bytes.NewReader(src), name)
	if err != nil {
		return err
	}
	proto, err := lua.Compile(chunk, name)
	if err != nil {
		return err
	}
	L.SetField(preload, name, L.NewFunctionFromProto(proto))
	return nil
}

package sqlgen

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

const (
	STMT_IGNERR uint32 = 1 << iota
	STMT_QUERY
	STMT_SORTED
	STMT_PREPARED
)

type Stmt struct {
	Flags  uint32
	Query  string
	Params []interface{}
}

func (stmt Stmt) String() string {
	buf := new(strings.Builder)
	buf.WriteString(stmt.Query + ";")

	flags := make([]string, 0, 4)
	for _, flag := range []struct {
		name  string
		value uint32
	}{
		{"IGNERR", STMT_IGNERR},
		{"QUERY", STMT_QUERY},
		{"SORTED", STMT_SORTED},
		{"PREPARED", STMT_PREPARED},
	} {
		if flag.value&stmt.Flags > 0 {
			flags = append(flags, flag.name)
		}
	}
	if len(stmt.Params) > 0 || len(flags) > 0 {
		buf.WriteString(" --")
		if len(stmt.Params) > 0 {
			buf.WriteString(" params: ")
			params, _ := json.Marshal(stmt.Params)
			buf.Write(params)
		}
		if len(flags) > 0 {
			buf.WriteString(" flags: ")
			buf.WriteString(strings.Join(flags, "|"))
		}
	}
	return buf.String()
}

func (stmt *Stmt) registerLuaGlobal(vm *lua.LState, out io.Writer) {
	addFlagFn := func(value uint32) *lua.LFunction {
		return vm.NewFunction(func(L *lua.LState) int {
			stmt.Flags |= value
			return 0
		})
	}
	for _, f := range []struct {
		name  string
		value uint32
	}{
		{"stmt_ignerr", STMT_IGNERR},
		{"stmt_query", STMT_QUERY},
		{"stmt_sorted", STMT_SORTED},
		{"stmt_prepared", STMT_PREPARED},
	} {
		vm.SetGlobal(f.name, addFlagFn(f.value))
	}
	vm.SetGlobal("stmt_param", vm.NewFunction(func(L *lua.LState) int {
		v := L.Get(1)
		if v == lua.LNil {
			stmt.Params = append(stmt.Params, nil)
			out.Write([]byte{'?'})
			return 0
		}
		switch x := v.(type) {
		case lua.LNumber:
			vf, vi := float64(x), int64(x)
			if math.Abs(vf-float64(vi)) < 1e-8 {
				stmt.Params = append(stmt.Params, vi)
			} else {
				stmt.Params = append(stmt.Params, vf)
			}
		case lua.LString:
			stmt.Params = append(stmt.Params, string(x))
		case lua.LBool:
			stmt.Params = append(stmt.Params)
		default:
			vm.ArgError(1, fmt.Sprintf("%s :: %s", v.String(), v.Type().String()))
		}
		out.Write([]byte{'?'})
		return 0
	}))
	vm.SetGlobal("stmt_add_params", vm.NewFunction(func(L *lua.LState) int {
		for i := 1; i <= L.GetTop(); i++ {
			v := L.Get(i)
			if v == lua.LNil || v == nil {
				stmt.Params = append(stmt.Params, nil)
				continue
			}
			switch x := v.(type) {
			case lua.LNumber:
				vf, vi := float64(x), int64(x)
				if math.Abs(vf-float64(vi)) < 1e-8 {
					stmt.Params = append(stmt.Params, vi)
				} else {
					stmt.Params = append(stmt.Params, vf)
				}
			case lua.LString:
				stmt.Params = append(stmt.Params, string(x))
			case lua.LBool:
				stmt.Params = append(stmt.Params)
			default:
				vm.ArgError(i, fmt.Sprintf("%s :: %s", v.String(), v.Type().String()))
			}
		}
		return 0
	}))
}

func (stmt *Stmt) setQuery(query string) {
	if stmt.Flags&STMT_QUERY == 0 && isQuery(query) {
		stmt.Flags |= STMT_QUERY
	}
	if stmt.Flags&STMT_PREPARED == 0 && len(stmt.Params) > 0 {
		stmt.Flags |= STMT_PREPARED
	}
	stmt.Query = query
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

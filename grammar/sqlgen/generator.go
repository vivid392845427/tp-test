package sqlgen

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/go-randgen/grammar/parser"
	lua "github.com/yuin/gopher-lua"
)

type KeyFuncs map[string]func() (string, error)

func (k KeyFuncs) Gen(key string) (string, bool, error) {
	if kf, ok := k[key]; ok {
		if res, err := kf(); err != nil {
			return res, true, err
		} else {
			return res, true, nil
		}
	}
	return "", false, nil
}

type BranchAnalyze struct {
	NonTerminal string
	// serial number of this branch
	Branch int
	// confilct number of this branch
	Conflicts int
	// the content expanded by this branch
	Content string
	// one of confict sqls
	ExampleSql string
}

// return false means normal stop visit
type SqlVisitor func(sql string) bool

// visit fixed times to get `times` sqls
func FixedTimesVisitor(f func(i int, sql string), times int) SqlVisitor {
	i := 0
	return func(sql string) bool {
		f(i, sql)
		i++
		if i == times {
			return false
		}
		return true
	}
}

// SQLIterator is a iterator interface of sql generator
// SQLIterator is not thread safe
type SQLIterator interface {
	// Visit sql cases in iterator
	Visit(visitor SqlVisitor) error

	// you should call it in Visit callback, because it will be deleted after visit the sql
	PathInfo() *PathInfo
}

type PathInfo struct {
	Depth         int
	ProductionSet *ProductionSet
	SeqSet        *SeqSet
}

func newPathInfo() *PathInfo {
	return &PathInfo{
		ProductionSet: newProductionSet(),
		SeqSet:        newSeqSet(),
	}
}

func (p *PathInfo) clear() {
	p.ProductionSet.clear()
	p.SeqSet.clear()
}

// SQLRandomlyIterator is a iterator of sql generator
// note that it is not thread safe
type StmtGenerator struct {
	productionName string
	productionMap  map[string]*parser.Production
	keyFuncs       KeyFuncs
	luaVM          *lua.LState
	printBuf       *bytes.Buffer
	stmtCtx        *stmtContext
	rng            *rand.Rand
	maxRecursive   int
	debug          bool
}

type stmtContext struct {
	buf  *bytes.Buffer
	path *PathInfo
	stmt Stmt
}

func (ctx *stmtContext) reset(vm *lua.LState, out io.Writer) Stmt {
	stmt := ctx.stmt
	stmt.setQuery(ctx.buf.String())
	ctx.buf.Reset()
	ctx.path.clear()
	ctx.stmt = Stmt{}
	ctx.stmt.registerLuaGlobal(vm, out)
	return stmt
}

func newStmtContext() *stmtContext {
	return &stmtContext{buf: new(bytes.Buffer), path: newPathInfo()}
}

func NewGenerator(yy string, fs KeyFuncs, setup func(*lua.LState, io.Writer) error) (*StmtGenerator, error) {
	cs, ps, err := parser.Parse(parser.Tokenize(&parser.RuneSeq{Runes: []rune(yy), Pos: 0}))
	if err != nil {
		return nil, err
	}
	pm := make(map[string]*parser.Production, len(ps))
	for _, p := range ps {
		if pp, ok := pm[p.Head.OriginString()]; ok {
			pp.Alter = append(pp.Alter, p.Alter...)
			pm[p.Head.OriginString()] = pp
			continue
		}
		pm[p.Head.OriginString()] = p
	}
	it := &StmtGenerator{
		productionMap: pm,
		keyFuncs:      fs,
		luaVM:         lua.NewState(),
		printBuf:      new(bytes.Buffer),
		stmtCtx:       newStmtContext(),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
		maxRecursive:  15,
	}
	if err = setup(it.luaVM, it.printBuf); err != nil {
		return nil, err
	}
	for _, c := range cs {
		if err := it.luaVM.DoString(c.OriginString()[1 : len(c.OriginString())-1]); err != nil {
			return nil, err
		}
	}
	it.stmtCtx.reset(it.luaVM, it.printBuf)
	return it, nil
}

func (gen *StmtGenerator) SetRoot(root string) *StmtGenerator {
	gen.productionName = root
	return gen
}

func (gen *StmtGenerator) SetRand(rand *rand.Rand) *StmtGenerator {
	gen.rng = rand
	return gen
}

func (gen *StmtGenerator) SetDebug(enabled bool) *StmtGenerator {
	gen.debug = enabled
	return gen
}

func (gen *StmtGenerator) SetRecurLimit(limit int) *StmtGenerator {
	gen.maxRecursive = limit
	return gen
}

func (gen *StmtGenerator) PathInfo() *PathInfo {
	return gen.stmtCtx.path
}

func (gen *StmtGenerator) Walk(callback func(stmt Stmt) bool) error {
	_, err := gen.generate(gen.productionName, newLinkedMap(), false, callback)
	if err == nil || err == normalStop {
		return nil
	}
	return err
}

func (gen *StmtGenerator) Generate() ([]Stmt, error) {
	stmts := make([]Stmt, 0, 32)
	err := gen.Walk(func(stmt Stmt) bool {
		stmts = append(stmts, stmt)
		return true
	})
	if err != nil {
		return nil, err
	}
	return stmts, nil
}

// GenerateSQLSequentially returns a `SQLSequentialIterator` which can generate sql case by case randomly
// productions is a `Production` array created by `parser.Parse`
// productionName assigns a production name as the root node
// maxRecursive is max bnf extend recursive number in sql generation
// analyze flag is to open root cause analyze feature
// if debug is true, the iterator will print all paths during generation
func GenerateSQLRandomly(headCodeBlocks []*parser.CodeBlock,
	productionMap map[string]*parser.Production,
	keyFuncs KeyFuncs, productionName string, maxRecursive int,
	rng *rand.Rand, debug bool) (SQLIterator, error) {
	return nil, errors.New("deprecated")
}

var normalStop = errors.New("generateSQLRandomly: normal stop visit")

func (gen *StmtGenerator) printDebugInfo(word string, path *linkedMap) {
	if gen.debug {
		log.Printf("word `%s` path: %v\n", word, path.order)
	}
}

func willRecursive(seq *parser.Seq, set map[string]bool) bool {
	for _, item := range seq.Items {
		if parser.IsTknNonTerminal(item) && set[item.OriginString()] {
			return true
		}
	}
	return false
}

func (gen *StmtGenerator) generate(productionName string,
	recurCounter *linkedMap, parentPreSpace bool,
	callback func(stmt Stmt) bool) (hasWrite bool, err error) {
	gen.stmtCtx.path.Depth += 1
	defer func() { gen.stmtCtx.path.Depth -= 1 }()
	// get root production
	production, exist := gen.productionMap[productionName]
	if !exist {
		return false, fmt.Errorf("Production '%s' not found", productionName)
	}
	gen.stmtCtx.path.ProductionSet.add(production)

	// check max recursive count
	recurCounter.enter(productionName)
	defer func() {
		recurCounter.leave(productionName)
	}()
	if recurCounter.m[productionName] > gen.maxRecursive {
		return false, fmt.Errorf("`%s` expression recursive num exceed max loop back %d\n %v",
			productionName, gen.maxRecursive, recurCounter.order)
	}
	nearMaxRecur := make(map[string]bool)
	for name, count := range recurCounter.m {
		if count == gen.maxRecursive {
			nearMaxRecur[name] = true
		}
	}
	selectableSeqs, totalWeight := make([]*parser.Seq, 0), .0
	for _, seq := range production.Alter {
		if seq.Weight > 0 && !willRecursive(seq, nearMaxRecur) {
			selectableSeqs = append(selectableSeqs, seq)
			totalWeight += seq.Weight
		}
	}
	if len(selectableSeqs) == 0 {
		return false, fmt.Errorf("recursive num exceed max loop back %d\n %v",
			gen.maxRecursive, recurCounter.order)
	}

	// random an alter
	selectIndex, thisWeight, targetWeight := 0, .0, gen.rng.Float64()*totalWeight
	for ; selectIndex < len(selectableSeqs); selectIndex++ {
		thisWeight += selectableSeqs[selectIndex].Weight
		if thisWeight >= targetWeight {
			break
		}
	}
	seqs := selectableSeqs[selectIndex]
	gen.stmtCtx.path.SeqSet.add(seqs)
	firstWrite := true

	for _, item := range seqs.Items {
		if parser.IsTerminal(item) || parser.NonTerminalNotInMap(gen.productionMap, item) {
			// terminal
			gen.printDebugInfo(item.OriginString(), recurCounter)

			// semicolon
			if item.OriginString() == ";" {
				if !callback(gen.stmtCtx.reset(gen.luaVM, gen.printBuf)) {
					return !firstWrite, normalStop
				}
				firstWrite = true
				continue
			}

			if err = handlePreSpace(firstWrite, parentPreSpace, item, gen.stmtCtx.buf); err != nil {
				return !firstWrite, err
			}

			if _, err := gen.stmtCtx.buf.WriteString(item.OriginString()); err != nil {
				return !firstWrite, err
			}

			firstWrite = false

		} else if parser.IsKeyword(item) {
			if err = handlePreSpace(firstWrite, parentPreSpace, item, gen.stmtCtx.buf); err != nil {
				return !firstWrite, err
			}

			// key word parse
			if res, ok, err := gen.keyFuncs.Gen(item.OriginString()); err != nil {
				return !firstWrite, err
			} else if ok {
				gen.printDebugInfo(res, recurCounter)
				_, err := gen.stmtCtx.buf.WriteString(res)
				if err != nil {
					return !firstWrite, errors.New("fail to write `io.StringWriter`")
				}

				firstWrite = false
			} else {
				return !firstWrite, fmt.Errorf("'%s' key word not support", item.OriginString())
			}
		} else if parser.IsCodeBlock(item) {
			if err = handlePreSpace(firstWrite, parentPreSpace, item, gen.stmtCtx.buf); err != nil {
				return !firstWrite, err
			}

			// lua code block
			if err := gen.luaVM.DoString(item.OriginString()[1 : len(item.OriginString())-1]); err != nil {
				log.Printf("lua code `%s`, run fail\n %v\n",
					item.OriginString(), err)
				return !firstWrite, err
			}
			if gen.printBuf.Len() > 0 {
				gen.printDebugInfo(gen.printBuf.String(), recurCounter)
				gen.stmtCtx.buf.WriteString(gen.printBuf.String())
				gen.printBuf.Reset()
				firstWrite = false
			}
		} else {
			// nonTerminal recursive
			var hasSubWrite bool
			if firstWrite {
				hasSubWrite, err = gen.generate(item.OriginString(), recurCounter, parentPreSpace, callback)
			} else {
				hasSubWrite, err = gen.generate(item.OriginString(), recurCounter, item.HasPreSpace(), callback)
			}

			if firstWrite && hasSubWrite {
				firstWrite = false
			}

			if err != nil {
				return !firstWrite, err
			}
		}
	}
	return !firstWrite, nil
}

func handlePreSpace(firstWrite bool, parentSpace bool, tkn parser.Token, writer io.StringWriter) error {
	if firstWrite {
		if parentSpace {
			if err := writePreSpace(writer); err != nil {
				return errors.New("fail to write `io.StringWriter`")
			}
		}
		return nil
	}

	if tkn.HasPreSpace() {
		if err := writePreSpace(writer); err != nil {
			return errors.New("fail to write `io.StringWriter`")
		}
	}

	return nil
}

func writePreSpace(writer io.StringWriter) error {
	if _, err := writer.WriteString(" "); err != nil {
		return err
	}

	return nil
}

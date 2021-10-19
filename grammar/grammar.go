package grammar

import (
	"math/rand"

	"github.com/pingcap/go-randgen/grammar/parser"
	"github.com/pingcap/go-randgen/grammar/sqlgen"
)

func NewIter(yy string, root string, maxRecursive int,
	keyFuncs sqlgen.KeyFuncs, debug bool) (sqlgen.SQLIterator, error) {
	return NewIterWithRand(yy, root, maxRecursive, keyFuncs, nil, debug)
}

// Get Iterator by yy. The same rand could guarantee the same result.
// Note that this iterator is not thread safe
func NewIterWithRand(yy string, root string, maxRecursive int,
	keyFuncs sqlgen.KeyFuncs, rng *rand.Rand, debug bool) (sqlgen.SQLIterator, error) {

	codeblocks, _, productionMap, err := Parse(yy)
	if err != nil {
		return nil, err
	}

	sqlIter, err := sqlgen.GenerateSQLRandomly(codeblocks,
		productionMap, keyFuncs, root, maxRecursive, rng, debug)
	if err != nil {
		return nil, err
	}

	return sqlIter, nil
}

func initProductionMap(productions []*parser.Production) map[string]*parser.Production {
	// Head string -> production
	productionMap := make(map[string]*parser.Production)
	for _, production := range productions {
		if pm, exist := productionMap[production.Head.OriginString()]; exist {
			pm.Alter = append(pm.Alter, production.Alter...)
			productionMap[production.Head.OriginString()] = pm
			continue
		}
		productionMap[production.Head.OriginString()] = production
	}

	return productionMap
}

func Parse(yy string) ([]*parser.CodeBlock, []*parser.Production,
	map[string]*parser.Production, error) {
	reader := &parser.RuneSeq{Runes: []rune(yy), Pos: 0}
	codeblocks, productions, err := parser.Parse(parser.Tokenize(reader))
	if err != nil {
		return nil, nil, nil, err
	}

	return codeblocks, productions, initProductionMap(productions), nil
}

package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/go-randgen/grammar/sql_generator"

	. "github.com/zyguan/xs"
)

func schemaRule() Rule {

	collate := Seq("collate", OneOf("utf8_general_ci", "utf8mb4_bin"))

	pk := OneOf(
		", primary key (c_int), key (c_string)",
		", primary key (c_int), unique key (c_string)",
		", primary key (c_int), unique key (c_int, c_string)",
	)

	keys := Seq(
		OneOf(pk, ", primary key (c_int, c_string), unique key (c_string)"),
		", key (c_enum), key (c_set), key (c_timestamp), key (c_datetime), key (c_decimal)",
	)

	return OneOf(
		Seq("create table t (",
			"c_int int,",
			"c_double double,",
			"c_decimal decimal(12,6),",
			"c_string varchar(40)", OneOf(Empty(), collate), ",",
			"c_datetime datetime,",
			"c_timestamp timestamp,",
			"c_enum enum('a', 'b', 'c', 'd', 'e'),",
			"c_set set('1', '2', '3', '4', '5'),",
			"c_json json",
			OneOf(Empty(), keys),
			")"),
		Seq("create table t (",
			"c_int int auto_increment,",
			"c_double double,",
			"c_decimal decimal(12,6),",
			"c_string varchar(40)", OneOf(Empty(), collate), ",",
			"c_datetime datetime,",
			"c_timestamp timestamp,",
			"c_enum enum('a', 'b', 'c', 'd', 'e'),",
			"c_set set('1', '2', '3', '4', '5'),",
			"c_json json",
			keys,
			")"),
		// TODO: add auto_random schemas for tidb-only test
	)
}

func randInt() int { return rand.Intn(20) + 1 }

func randDouble() float64 { return rand.Float64() }

func randDecimal() float64 { return float64(rand.Intn(10000)) / 100 }

func randString() string {
	var adjectives = []string{
		"black",
		"white",
		"gray",
		"brown",
		"red",
		"pink",
		"crimson",
		"carnelian",
		"orange",
		"yellow",
		"ivory",
		"cream",
		"green",
		"cyan",
		"blue",
		"cerulean",
		"azure",
		"indigo",
		"navy",
		"violet",
		"purple",
		"dark",
		"light",
		"gold",
		"silver",
	}
	var nouns = []string{
		"head",
		"crest",
		"crown",
		"tooth",
		"sight",
		"seer",
		"speaker",
		"singer",
		"song",
		"master",
		"mistress",
		"witch",
		"warlock",
		"warrior",
		"jester",
		"paladin",
		"bard",
		"trader",
		"scourge",
		"watcher",
		"cat",
		"vulture",
		"spider",
		"fly",
		"koala",
		"kangaroo",
		"yak",
		"sloth",
		"ant",
		"roach",
		"carpet",
		"curtain",
	}
	return adjectives[rand.Intn(len(adjectives))] + " " + nouns[rand.Intn(len(nouns))]
}

func randDatetime() string {
	n := rand.Intn(7 * 24)
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if n%2 == 0 {
		base = base.Add(time.Duration(n) * time.Hour)
	} else {
		base = base.Add(-time.Duration(n) * time.Hour)
	}
	return base.Format("2006-01-02 15:04:05")
}

func randTimestamp() string {
	n := rand.Intn(7 * 24 * 3600)
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if n%2 == 0 {
		base = base.Add(-time.Duration(n) * time.Second)
	} else {
		base = base.Add(time.Duration(n) * time.Second)
	}
	return base.Format("2006-01-02 15:04:05")
}

func randEnum() string {
	return []string{"a", "b", "c", "d", "e"}[rand.Intn(5)]
}

func randSet() string {
	n := rand.Intn(1 << 5)
	set := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		if n == 0 {
			break
		}
		if n&1 > 0 {
			set = append(set, strconv.Itoa(i+1))
		}
		n = n >> 1
	}
	return strings.Join(set, ",")
}

func randJson() string {
	return fmt.Sprintf(`{"int":%d,"str":"%s","datetime":"%s","enum":"%s","set":"%s"}`,
		randInt(), randString(), randDatetime(), randEnum(), randSet())
}

func registerSchemaFuncs(fs sql_generator.KeyFuncs) sql_generator.KeyFuncs {
	if fs == nil {
		fs = make(sql_generator.KeyFuncs)
	}
	fs["__c_int__"] = func() (s string, err error) {
		return strconv.Itoa(randInt()), nil
	}
	fs["__c_double__"] = func() (s string, err error) {
		return fmt.Sprintf("%.6f", randDouble()), nil
	}
	fs["__c_decimal__"] = func() (s string, err error) {
		return fmt.Sprintf("%.2f", randDecimal()), nil
	}
	fs["__c_string__"] = func() (s string, err error) {
		return "'" + randString() + "'", nil
	}
	fs["__c_datetime__"] = func() (s string, err error) {
		return "'" + randDatetime() + "'", nil
	}
	fs["__c_timestamp__"] = func() (s string, err error) {
		return "'" + randTimestamp() + "'", nil
	}
	fs["__c_enum__"] = func() (s string, err error) {
		return "'" + randEnum() + "'", nil
	}
	fs["__c_set__"] = func() (s string, err error) {
		return "'" + randSet() + "'", nil
	}
	fs["__c_json__"] = func() (s string, err error) {
		return "'" + randJson() + "'", nil
	}
	return fs
}

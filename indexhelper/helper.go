package indexhelper

import (
	"context"
)

type Adapter interface {
	Close() error

	FetchIndexInfo(context.Context) error
}

type Database struct {
	Name   string
	Tables []*Table
}

type Table struct {
	Name    string
	DBName  string
	Columns []*Column
	Indexes []*Index
}

type Column struct {
	Name      string
	DBName    string
	TableName string
	Type      string
	AllowNull bool
}

type Index struct {
	Name      string
	DBName    string
	TableName string
	IsUnique  bool
	Columns   []string
}

func InArray(ary []string, subAry []string) bool {
	n := 0
	for _, sa := range subAry {
		for _, a := range ary {
			if a == sa {
				n++
				break
			}
		}
	}

	return n == len(subAry)
}

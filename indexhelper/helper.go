package indexhelper

import (
	"context"
	"fmt"
)

type Adapter interface {
	Close() error

	FetchIndexInfo(context.Context) error
	GetNotGoodItems(context.Context) []*NotGoodItem
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
	Name        string
	DBName      string
	TableName   string
	IsUnique    bool
	Columns     []string
	Cardinality []uint64
}

type NotGoodItem struct {
	Name   string
	Detail string
}

func (d *Database) String() string {
	return d.Name
}

func (t *Table) String() string {
	return fmt.Sprintf("%s.%s", t.DBName, t.Name)
}

func (c *Column) String() string {
	return fmt.Sprintf("%s.%s.%s", c.DBName, c.TableName, c.Name)
}

func (i *Index) String() string {
	return fmt.Sprintf("%s.%s.%s", i.DBName, i.TableName, i.Name)
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

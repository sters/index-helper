package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	// load mysql driver for this package.
	_ "github.com/go-sql-driver/mysql"

	"github.com/morikuni/failure"
	"github.com/sters/index-helper/indexhelper"
)

type Adapter struct {
	client *sql.DB
	loaded loadResult

	overwrapIndexes            map[string]map[string][]*overwrap
	foreignIndexes             map[string]map[string][]*indexhelper.Index
	noForeignIndexColumns      map[string]map[string][]*indexhelper.Column
	badCardinalityOrderIndexes map[string]map[string][]*indexhelper.Index
}

type overwrap struct {
	coveredIndex *indexhelper.Index
	smallIndexes []*indexhelper.Index
}

type loadResult map[string]map[string]*indexhelper.Table

const (
	connMaxLifetime = 3 * time.Minute
	maxOpenConns    = 2
	maxIdleConns    = 1
)

func Open(user, password, host string) (*Adapter, error) {
	db, err := sql.Open(
		"mysql",
		fmt.Sprintf("%s:%s@tcp(%s)/information_schema", user, password, host),
	)
	if err != nil {
		return nil, failure.Wrap(err)
	}

	// See "Important settings" section.
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)

	return &Adapter{
		client: db,
		loaded: loadResult{},
	}, nil
}

func (m *Adapter) Close() error {
	return failure.Wrap(m.client.Close())
}

func (m *Adapter) FetchIndexInfo(ctx context.Context) error {
	if err := m.loadColumnList(ctx); err != nil {
		return failure.Wrap(err)
	}

	if err := m.loadIndexList(ctx); err != nil {
		return failure.Wrap(err)
	}

	m.findOverWrapIndex()
	m.findForeignIndex()
	m.findBadCardinalityOrderIndex()

	return nil
}

func (m *Adapter) loadColumnList(ctx context.Context) error {
	const query = `
select
table_schema,
table_name,
column_name,
column_type,
is_nullable

from
information_schema.columns`

	rows, err := m.client.QueryContext(ctx, query)
	if err != nil {
		return failure.Wrap(err)
	}

	defer rows.Close()
	for rows.Next() {
		var r indexhelper.Column
		var allowNull string
		err := rows.Scan(
			&r.DBName,
			&r.TableName,
			&r.Name,
			&r.Type,
			&allowNull,
		)
		if err != nil {
			return failure.Wrap(err)
		}

		r.AllowNull = false
		if allowNull == "YES" {
			r.AllowNull = true
		}

		if _, ok := m.loaded[r.DBName]; !ok {
			m.loaded[r.DBName] = make(map[string]*indexhelper.Table)
		}

		if _, ok := m.loaded[r.DBName][r.TableName]; !ok {
			m.loaded[r.DBName][r.TableName] = &indexhelper.Table{
				Name:   r.TableName,
				DBName: r.DBName,
			}
		}

		m.loaded[r.DBName][r.TableName].Columns = append(
			m.loaded[r.DBName][r.TableName].Columns,
			&r,
		)
	}

	if err := rows.Err(); err != nil {
		return failure.Wrap(err)
	}

	return nil
}

func (m *Adapter) loadIndexList(ctx context.Context) error {
	const query = `
select
table_schema,
table_name,
index_name,
non_unique,
group_concat(column_name order by seq_in_index, ","),
group_concat(cardinality order by seq_in_index, ",")

from
information_schema.statistics

group by
table_schema, table_name, index_name

order by
table_schema, table_name, non_unique, index_name`

	rows, err := m.client.QueryContext(ctx, query)
	if err != nil {
		return failure.Wrap(err)
	}

	defer rows.Close()
	for rows.Next() {
		var r indexhelper.Index
		var nonUnique int
		var columns string
		var cardinalities *string
		err := rows.Scan(
			&r.DBName,
			&r.TableName,
			&r.Name,
			&nonUnique,
			&columns,
			&cardinalities,
		)
		if err != nil {
			return failure.Wrap(err)
		}

		r.IsUnique = nonUnique == 0
		r.Columns = strings.Split(columns, ",")

		if cardinalities != nil {
			cards := strings.Split(*cardinalities, ",")
			r.Cardinality = make([]uint64, len(cards))
			for i, c := range cards {
				cc, err := strconv.ParseUint(c, 10, 64) // nolint:gomnd
				if err != nil {
					continue
				}
				r.Cardinality[i] = cc
			}
		}

		if _, ok := m.loaded[r.DBName]; !ok {
			m.loaded[r.DBName] = make(map[string]*indexhelper.Table)
		}

		if _, ok := m.loaded[r.DBName][r.TableName]; !ok {
			m.loaded[r.DBName][r.TableName] = &indexhelper.Table{
				Name:   r.TableName,
				DBName: r.DBName,
			}
		}

		m.loaded[r.DBName][r.TableName].Indexes = append(
			m.loaded[r.DBName][r.TableName].Indexes,
			&r,
		)
	}

	if err := rows.Err(); err != nil {
		return failure.Wrap(err)
	}

	return nil
}

func (m *Adapter) findOverWrapIndex() {
	// TODO: 先頭が一致していて、片方が長い場合は確実にoverwrapされている
	// TODO: 先頭以外で一致している場合は、WHEREにカラムを足すことでカバーできる可能性がある
	// TODO: 先頭が一致していて、お尻が異なる場合は難しいけど対応できるかもしれない
	// https://github.com/mysql/mysql-sys#schema_redundant_indexes--xschema_flattened_keys これでええやん
	for dbName, db := range m.loaded {
		for _, table := range db {
			table := table
			sort.Slice(table.Indexes, func(i, j int) bool {
				return len(table.Indexes[i].Columns) > len(table.Indexes[j].Columns)
			})

			exists := []*overwrap{}

			for _, i := range table.Indexes {
				if i.Name == "PRIMARY" {
					continue
				}

				isContinue := false
				for _, ei := range exists {
					if indexhelper.InArray(ei.coveredIndex.Columns, i.Columns) {
						ei.smallIndexes = append(ei.smallIndexes, i)
						isContinue = true
						break
					}
				}
				if isContinue {
					continue
				}

				exists = append(
					exists,
					&overwrap{
						coveredIndex: i,
					},
				)
			}

			if m.overwrapIndexes == nil {
				m.overwrapIndexes = make(map[string]map[string][]*overwrap)
			}
			if _, ok := m.overwrapIndexes[dbName]; !ok {
				m.overwrapIndexes[dbName] = make(map[string][]*overwrap)
			}
			m.overwrapIndexes[dbName][table.Name] = exists
		}
	}
}

func (m *Adapter) findForeignIndex() {
	for dbName, db := range m.loaded {
		for _, table := range db {
			table := table
			sort.Slice(table.Indexes, func(i, j int) bool {
				return len(table.Indexes[i].Columns) > len(table.Indexes[j].Columns)
			})

			indexes := make(map[string]struct{})
			exists := []*indexhelper.Index{}
			for _, i := range table.Indexes {
				found := false
				for _, c := range i.Columns {
					// todo
					if strings.Contains(c, "_id") {
						indexes[c] = struct{}{}
						found = true
					}
				}

				if found {
					exists = append(exists, i)
				}
			}

			if m.foreignIndexes == nil {
				m.foreignIndexes = make(map[string]map[string][]*indexhelper.Index)
			}
			if _, ok := m.foreignIndexes[dbName]; !ok {
				m.foreignIndexes[dbName] = make(map[string][]*indexhelper.Index)
			}
			m.foreignIndexes[dbName][table.Name] = exists

			columns := []*indexhelper.Column{}
			for _, c := range table.Columns {
				// todo
				if _, ok := indexes[c.Name]; ok || !strings.Contains(c.Name, "_id") {
					continue
				}
				columns = append(columns, c)
			}

			if m.noForeignIndexColumns == nil {
				m.noForeignIndexColumns = make(map[string]map[string][]*indexhelper.Column)
			}
			if _, ok := m.noForeignIndexColumns[dbName]; !ok {
				m.noForeignIndexColumns[dbName] = make(map[string][]*indexhelper.Column)
			}
			m.noForeignIndexColumns[dbName][table.Name] = columns
		}
	}
}

func (m *Adapter) findBadCardinalityOrderIndex() {
	// TODO: cardinalityはやっぱり計算された結果になるので、COUNT(DISTINCT hoge) しないとちゃんと出てこない
	for dbName, db := range m.loaded {
		for _, table := range db {
			exists := []*indexhelper.Index{}
			for _, i := range table.Indexes {
				if len(i.Cardinality) == 1 {
					continue
				}

				found := false
				before := uint64(0)
				total := uint64(0)
				for i, c := range i.Cardinality {
					if i > 0 && before < (c-total) {
						found = true
						break
					}
					before = (c - total)
					total = c
				}

				if found {
					exists = append(exists, i)
				}
			}

			if m.badCardinalityOrderIndexes == nil {
				m.badCardinalityOrderIndexes = make(map[string]map[string][]*indexhelper.Index)
			}
			if _, ok := m.badCardinalityOrderIndexes[dbName]; !ok {
				m.badCardinalityOrderIndexes[dbName] = make(map[string][]*indexhelper.Index)
			}
			m.badCardinalityOrderIndexes[dbName][table.Name] = exists
		}
	}
}

/*
select
c.table_schema,
c.table_name,
c.column_name,
c.column_type,
t.table_rows

from
information_schema.columns as c
inner join
information_schema.tables as t
on c.table_schema = t.table_schema and c.table_name = t.table_name

where
c.table_schema = "xxx" and c.column_key = "PRI"

order by
table_rows desc
*/

func (m *Adapter) GetNotGoodItems(context.Context) []*indexhelper.NotGoodItem {
	result := []*indexhelper.NotGoodItem{}

	for _, indexes := range m.overwrapIndexes {
		for _, table := range indexes {
			for _, o := range table {
				for _, si := range o.smallIndexes {
					result = append(
						result,
						&indexhelper.NotGoodItem{
							Name: fmt.Sprintf(
								"Index %s is covered by another index %s",
								si,
								o.coveredIndex,
							),
						},
					)
				}
			}
		}
	}

	for _, table := range m.noForeignIndexColumns {
		for _, cols := range table {
			for _, c := range cols {
				result = append(
					result,
					&indexhelper.NotGoodItem{
						Name: fmt.Sprintf(
							"Column %s seems foreign key but not indexed",
							c,
						),
					},
				)
			}
		}
	}

	for _, table := range m.badCardinalityOrderIndexes {
		for _, indexes := range table {
			for _, i := range indexes {
				result = append(
					result,
					&indexhelper.NotGoodItem{
						Name: fmt.Sprintf(
							"Index %s has bad cardinality order",
							i,
						),
					},
				)
			}
		}
	}

	return result
}

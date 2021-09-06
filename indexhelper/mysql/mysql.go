package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	_ "github.com/go-sql-driver/mysql"
	"github.com/morikuni/failure"
	"github.com/sters/index-helper/indexhelper"
)

type Adapter struct {
	client *sql.DB
	loaded loadResult

	overwrapIndexes map[string]map[string][]*overwrap
}

type overwrap struct {
	coveredIndex *indexhelper.Index
	smallIndexes []*indexhelper.Index
}

type loadResult map[string]map[string]*indexhelper.Table

func Open(user, password, host string) (*Adapter, error) {
	db, err := sql.Open(
		"mysql",
		fmt.Sprintf("%s:%s@tcp(%s)/information_schema", user, password, host),
	)
	if err != nil {
		return nil, failure.Wrap(err)
	}

	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

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
			m.loaded[r.DBName] = map[string]*indexhelper.Table{}
		}

		if _, ok := m.loaded[r.DBName][r.TableName]; !ok {
			m.loaded[r.DBName][r.TableName] = &indexhelper.Table{
				Name:   r.Name,
				DBName: r.DBName,
			}
		}

		m.loaded[r.DBName][r.TableName].Columns = append(
			m.loaded[r.DBName][r.TableName].Columns,
			&r,
		)
	}

	spew.Dump(m)
	return nil
}

func (m *Adapter) loadIndexList(ctx context.Context) error {
	const query = `
select
table_schema,
table_name,
index_name,
non_unique,
group_concat(column_name order by seq_in_index, ",")

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
		var c string
		err := rows.Scan(
			&r.DBName,
			&r.TableName,
			&r.Name,
			&nonUnique,
			&c,
		)
		if err != nil {
			return failure.Wrap(err)
		}

		r.IsUnique = nonUnique == 0
		r.Columns = strings.Split(c, ",")

		if _, ok := m.loaded[r.DBName]; !ok {
			m.loaded[r.DBName] = map[string]*indexhelper.Table{}
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

	return nil
}

func (m *Adapter) findOverWrapIndex() {
	for dbName, db := range m.loaded {
		for _, table := range db {
			if table.Name != "access_limits" {
				continue
			}

			table := table
			sort.Slice(table.Indexes, func(i, j int) bool {
				return len(table.Indexes[i].Columns) > len(table.Indexes[j].Columns)
			})

			exists := []*overwrap{}

			for _, i := range table.Indexes {
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

	spew.Dump(m.overwrapIndexes)
}

package core

import (
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
)

type Schema struct {
	Database string
	Table    string
	Columns  []string
}

type SchemaCache struct {
	Schemas []*Schema
	DB      *sql.DB
}

func (sc *SchemaCache) Init(config *replication.BinlogSyncerConfig) {
	db, err := sql.Open("mysql", getDSN(config))
	if err != nil {
		fmt.Println(err)
		return
	}
	sc.DB = db
}

func (sc *SchemaCache) contains(schema *Schema) bool {
	for _, s := range sc.Schemas {
		if s.Database == schema.Database && s.Table == schema.Table && reflect.DeepEqual(s.Columns, schema.Columns) {
			return true
		}
	}
	return false
}

func (sc *SchemaCache) put(schema *Schema) {
	if !sc.contains(schema) {
		sc.Schemas = append(sc.Schemas, schema)
	} else {
		fmt.Println("Schema already exists, not inserting:", schema)
	}
}

func (sc *SchemaCache) Get(index int) (*Schema, error) {
	if index < 0 || index >= len(sc.Schemas) {
		return &Schema{}, fmt.Errorf("Index out of range")
	}
	return sc.Schemas[index], nil
}

func (sc *SchemaCache) getByTableAndDatabase(database, table string) (*Schema, error) {
	for _, s := range sc.Schemas {
		if s.Database == database && s.Table == table {
			return s, nil
		}
	}
	return nil, fmt.Errorf("Schema not found for Table: %s and Database: %s", table, database)
}

func (sc *SchemaCache) getColumns(dbName, tableName string) (*Schema, error) {
	s, _ := sc.getByTableAndDatabase(dbName, tableName)

	if s == nil {
		query := fmt.Sprintf("SELECT COLUMN_NAME fName FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", dbName, tableName)

		rows, err := sc.DB.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var columns []string

		for rows.Next() {
			var columnName string
			err := rows.Scan(&columnName)
			if err != nil {
				return nil, err
			}
			columns = append(columns, columnName)
		}
		s = &Schema{
			Database: dbName,
			Table:    tableName,
			Columns:  columns,
		}

		sc.put(s)
	}

	return s, nil
}

func getDSN(c *replication.BinlogSyncerConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		c.User,
		c.Password,
		c.Host,
		c.Port,
	)
}

package godb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Database struct {
	User                     string
	Password                 string
	DBType                   string
	Protocol                 string
	Host                     string
	Port                     string
	Name                     string
	dsn                      string
	sqlConnection            *sql.DB
	preparedJSONSQLStatement *sql.Stmt
	DSNArgs                  string
}

func (db *Database) Connect() error {
	switch db.DBType {
	case "mysql":
		dsn := SQLBuildDSN(db)
		newConnection, err := sql.Open(db.DBType, dsn)
		if err != nil {
			return err
		}
		db.dsn = dsn
		db.sqlConnection = newConnection
		return nil
	}
	return errors.New("No known database type or type not specified")
}
func SQLBuildDSN(db *Database) string {
	dsn := db.User + ":" + db.Password + "@" + db.Protocol + "(" + db.Host + ":" + db.Port + ")/" + db.Name + db.DSNArgs
	return dsn
}
func New(user, password, dbType, protocol, host, port, name, args string) *Database {
	return &Database{User: user, Password: password, DBType: dbType, Protocol: protocol, Host: host, Port: port, Name: name, dsn: "", sqlConnection: nil, DSNArgs: args}
}

func (db *Database) WriteJSON(where, id string, data interface{}) error {
	json, err := json.Marshal(data)
	if err != nil {
		return err
	}
	switch db.DBType {
	case "mysql":
		if db.preparedJSONSQLStatement == nil {
			stm := fmt.Sprintf("INSERT INTO %v VALUES(?,?,?) ON DUPLICATE KEY UPDATE data=?", where)
			insert, err := db.sqlConnection.Prepare(stm)
			if err != nil {
				return err
			}
			db.preparedJSONSQLStatement = insert
		}
		_, err = db.preparedJSONSQLStatement.Exec(fmt.Sprintf("%v", time.Now().Unix()), id, string(json), string(json))
		if err != nil {
			return err
		}
	}
	return nil
}
func (db Database) MYSQLCreateJSONTable(name string) error {
	stm := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (`timestamp` int(11) unsigned,`id` varchar(255),`data` JSON,PRIMARY KEY(`id` ));", db.Name, name)
	create, err := db.sqlConnection.Prepare(stm)
	if err != nil {
		return err
	}
	_, err = create.Exec()
	if err != nil {
		return err
	}
	return nil
}

package gdpr

import (
	"database/sql"
	"fmt"
	"log"
	"os/exec"

	_ "github.com/go-sql-driver/mysql"
)

var (
	DBUser = "tslilyai"
	DBPW   = "pass"

	// TABLE QUERIES
	createTableTextQ = `
	CREATE TABLE IF NOT EXISTS TextRecord (
		 id		BIGINT
		,parentid		BIGINT
		,version int 
		,content	text
		,UNIQUE(id)
	);`
	createTableVoteQ = `
	CREATE TABLE IF NOT EXISTS Vote (
		id		BIGINT
		,version	int 
		,isPos 		boolean	
		,UNIQUE(id)
	);`
	dropTableTextQ = `DROP TABLE IF EXISTS TextRecord`
	dropTableVoteQ = `DROP TABLE IF EXISTS Vote`
	// WRITE QUERIES
	newTextRecordsQ = `SELECT TextRecord.id, parentid, content, version FROM TextRecord
			WHERE TextRecord.version > ? ORDER BY version ASC;`
	
    newVotesQ = `SELECT Vote.id, isPos, version FROM Vote
			WHERE Vote.version > ?;`
	
    updateOrInsertVoteQ = `insert into Vote(id, version, isPos)
		   VALUES(?, ?, ?)
		   ON DUPLICATE KEY 
			UPDATE version = IF(version < ?, ?, version),
					isPos = IF(version < ?, ?, isPos);`

	updateOrInsertTextRecordsQ = `insert into TextRecord(id, parentid, content, version)
		   VALUES(?, ?, ?, ?)
		   ON DUPLICATE KEY 
			UPDATE version = IF(version < ?, ?, version),
					content = IF(version < ?, ?, content);`
	deleteTextRecordsQ = `DELETE FROM TextRecord WHERE id = ?;`
	deleteVoteQ        = `DELETE FROM Vote WHERE id = ?;`

	// READ QUERIES
	getMostRecentTextRecordsQ = `SELECT version 
		FROM TextRecord
		ORDER BY version DESC
		LIMIT 1;`
	getMostRecentVoteQ = `SELECT version 
		FROM Vote
		ORDER BY version DESC
		LIMIT 1;`
	getAllTextRecordsIdsQ = `SELECT id FROM TextRecord;`
	getAllVoteidsQ        = `SELECT id FROM Vote;`
)

func (u *User) DBInit() {
	name := fmt.Sprintf("user_%d", u.me)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/", DBUser, DBPW))
	if err != nil {
		log.Fatalf("Open error" + err.Error())
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("Ping error" + err.Error())
	}

	dropQ := fmt.Sprintf("drop database if exists %s;", name)
	u.DPrintf("dropping database!")
	if _, err = db.Exec(dropQ); err != nil {
		log.Fatalf("Drop error" + err.Error())
	}

	createQ := fmt.Sprintf("create database if not exists %s;", name)
	u.DPrintf("creating database!")
	if _, err = db.Exec(createQ); err != nil {
		log.Fatalf("Create error" + err.Error())
	}
	db.Close()

	if u.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/%s", DBUser, DBPW, name)); err != nil {
		log.Fatalf("Open error" + err.Error())
	}
	if err = u.db.Ping(); err != nil {
		log.Fatalf("Ping error" + err.Error())
	}
	u.DPrintf("Creating database tables")
	u.CreateTables()

	u.DPrintf("Importing data")
	var cmd *exec.Cmd
	cmd = exec.Command("sh", "-c", fmt.Sprintf("mysqlimport --lines-terminated-by='\n' --fields-terminated-by=',' --verbose --local -utslilyai -ppass user_%d data/%d/TextRecord.csv", u.me, u.me))
	if err := cmd.Run(); err != nil {
		log.Fatalf("Importing text: " + err.Error())
	}

	cmd = exec.Command("sh", "-c", fmt.Sprintf("mysqlimport --lines-terminated-by='\n' --fields-terminated-by=',' --verbose --local -utslilyai -ppass user_%d data/%d/Vote.csv", u.me, u.me))
	if err := cmd.Run(); err != nil {
		log.Fatalf("Importing vote: " + err.Error())
	}
}

func (u *User) DBGetAllIds() ([]int64, []int64) {
	textIds := []int64{}
	voteIds := []int64{}

	var id int64
	rows, err := u.db.Query(getAllTextRecordsIdsQ)
	if err != nil {
		log.Fatalf("GetAllTextRecordsIds: %s", err.Error())
	}
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			log.Fatalf(err.Error())
		}
		textIds = append(textIds, id)
	}
	rows, err = u.db.Query(getAllVoteidsQ)
	if err != nil {
		log.Fatalf("GetAllVoteids: %s", err.Error())
	}
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			log.Fatalf(err.Error())
		}
		voteIds = append(voteIds, id)
	}
	return textIds, voteIds
}

func (u *User) DBGetMostRecentVersion() int64 {
	max_ts := 0
	ts := 0
	rows, err := u.db.Query(getMostRecentTextRecordsQ)
	if err != nil {
		log.Fatalf("GetMostRecentTextRecords: %s", err.Error())
	}
	for rows.Next() {
		if err := rows.Scan(&ts); err != nil {
			log.Fatalf(err.Error())
		}
		if ts > max_ts {
			max_ts = ts
		}
	}
	if rows, err = u.db.Query(getMostRecentVoteQ); err != nil {
		log.Fatalf("GetMostRecentVote: %s", err.Error())
	}
	for rows.Next() {
		if err := rows.Scan(&ts); err != nil {
			log.Fatalf(err.Error())
		}
		if ts > max_ts {
			max_ts = ts
		}
	}
	return int64(ts)
}

func (u *User) GetDataSinceVersion(queryv int64) ([]Text, []Vote, error) {
	var (
		id      int64
		pid     int64
		content string
		isPos   bool
		ver     int64
		rows    *sql.Rows
		err     error
	)

	// get text rows
	newTextRecords := []Text{}
	rows, err = u.db.Query(newTextRecordsQ, queryv)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		if err := rows.Scan(&id, &pid, &content, &ver); err != nil {
			log.Fatalf(err.Error())
		}
        newTextRecords = append(newTextRecords, Text{User: u.me, Id: id, Parent: pid, Content: content, Version: ver})
	}
	rows.Close()

	// get vote rows
	newVotes := []Vote{}
	if rows, err = u.db.Query(newVotesQ, queryv); err != nil {
		return nil, nil, err
		log.Fatalf("GetDataSinceVote: %s", err.Error())
	}
	for rows.Next() {
		if err := rows.Scan(&id, &isPos, &ver); err != nil {
			log.Fatalf(err.Error())
		}
        newVotes = append(newVotes, Vote{User: u.me, Id: id, IsPos: isPos, Version: ver})
	}
	rows.Close()

	return newTextRecords, newVotes, nil
}

/* DELETIONS */
func (u *User) DeleteTextRecordsDB(id int64) error {
	_, err := u.db.Exec(deleteTextRecordsQ, id)
	return err
}

func (u *User) DeleteVoteDB(id int64) error {
	_, err := u.db.Exec(deleteVoteQ, id)
	return err
}

/*
	UPDATES AND INSERTS
*/
func (u *User) updateOrInsertTextRecordsDB(a Text) error {
	_, err := u.db.Exec(updateOrInsertTextRecordsQ, a.Id, a.Parent, a.Content, a.Version, a.Version, a.Version, a.Version, a.Content)
	return err
}
func (u *User) updateOrInsertVoteDB(a Vote) error {
	_, err := u.db.Exec(updateOrInsertVoteQ, a.Id, a.Version, a.IsPos, a.Version, a.Version, a.Version, a.IsPos)
	return err
}
func (u *User) InsertTextRecordsDB(a Text) error {
	return u.updateOrInsertTextRecordsDB(a)
}
func (u *User) UpdateTextRecordsDB(a Text) error {
	return u.updateOrInsertTextRecordsDB(a)
}
func (u *User) InsertVoteDB(a Vote) error {
	return u.updateOrInsertVoteDB(a)
}
func (u *User) UpdateVoteDB(a Vote) error {
	return u.updateOrInsertVoteDB(a)
}

/*
	Executable queries (does not return any database state)
*/
func (u *User) CreateTables() {
	if _, err := u.db.Exec(createTableTextQ); err != nil {
		log.Fatalf(err.Error())
	}
	if _, err := u.db.Exec(createTableVoteQ); err != nil {
		log.Fatalf(err.Error())
	}
}

func (u *User) DropTables() {
	if _, err := u.db.Exec(dropTableTextQ); err != nil {
		log.Fatalf(err.Error())
	}
	if _, err := u.db.Exec(dropTableVoteQ); err != nil {
		log.Fatalf(err.Error())
	}
	u.CreateTables()
}

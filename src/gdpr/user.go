package gdpr

import (
	"database/sql"
	"math/rand"
	"sync"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"

	"../labrpc"
)

type User struct {
	me                  int
	db                  *sql.DB
	app                 *labrpc.ClientEnd
	mu                  sync.Mutex
	dead                int32
	logger              *Logger
	currentVersion      int64
	serviceKnownVersion int64
	subscribed          int32

	/* persistent */
	logCheckpt int64
}

func MakeUser(me int, logger *Logger, app *labrpc.ClientEnd) *User {
	u := &User{}
	u.me = me
	u.app = app
	u.dead = 0
	u.logger = logger
	u.subscribed = 0
	u.serviceKnownVersion = -1 // initally -1 so any init updates are sent
	return u
}

func (u *User) Start() {
	u.DBInit()
	u.currentVersion = u.DBGetMostRecentVersion()

	u.Recover()
	u.Subscribe()
}

func (u *User) Recover() {
	// TODO use the most recent log checkpt
	// could use the most recent Version as proxy... hmm
	// this would require switching the order of updating service/db
	// probably best to just use file offset so that there's no awkward search

	// get relevant log entries
	logEntries := u.logger.ReadLogEntriesSince(0)
	maxVersion := u.currentVersion
	for i, entry := range logEntries {

		// send the server the update regardless,
		// will use version to filter out duplicates
		u.DPrintf("Sending server update %d from log!", i)
		err := u.SendAppServerUpdate(&entry)
		if err != OK {
			// somehow texts didn't exist?
			// don't add to database either
			continue
		}

		for _, text := range entry.Texts {
			if text.Version > u.currentVersion {
				// thsi text was added/inserted after the last known update
				// but what if the text has two log entries? and they're not ordered?
				// can this happen? nope --> query only updates if ts is greater!
				u.updateOrInsertTextRecordsDB(text)
			}
			maxVersion = max(text.Version, maxVersion)
		}
		for _, vote := range entry.Votes {
			if vote.Version > u.currentVersion {
				u.updateOrInsertVoteDB(vote)
			}
			maxVersion = max(vote.Version, maxVersion)
		}
	}
	u.currentVersion = maxVersion
}

func (u *User) UploadSinceVersion(version int64) Err {
	newTexts, newVotes, err := u.GetDataSinceVersion(version)
	if err != nil {
		u.DPrintf("Update since version %d, errDB")
		return ErrDB
	}
	u.mu.Lock()
	if u.serviceKnownVersion >= u.currentVersion {
		u.DPrintf("Update since version %d, known version %d >= current %d", version, u.serviceKnownVersion, u.currentVersion)
		u.mu.Unlock()
		return ErrUnauthorized
	}
	u.mu.Unlock()

	u.DPrintf("Update since version %d, %d, %d texts, %d votes",
		version, u.serviceKnownVersion, len(newTexts), len(newVotes))

	args := UpdateArgs{
		User:     u.me,
		Texts:    newTexts,
		Votes:    newVotes,
		IsDelete: false,
		IsUpload: true,
	}
	return u.SendAppServerUpdate(&args)
}

/*
 * Subscribe to AppServer, sending any data not yet uploaded
 * Arguments contain response to query containing all data since last update
 * sent to the server.
 */
func (u *User) Subscribe() {
	u.DPrintf("Subscribe called")
	// only subscribe if previously unsubscribed
	if atomic.LoadInt32(&u.subscribed) != 0 {
		return
	}
	err := u.UploadSinceVersion(-1)
	if err == OK {
		atomic.StoreInt32(&u.subscribed, 1)
		u.DPrintf("Subscribe succeeded")
	} else {
		u.DPrintf("Subscribe failed! %d", err)
	}
}

func (u *User) Unsubscribe() Err {
	atomic.StoreInt32(&u.subscribed, 0)

    /* Note that even though we check the status,
    *  there can still be a race (and the entry sent to the server)
    *  if the client is simultaneously sending entries and unsubscribing
    * 
    *  In particular, one client thread can check and see that it is subscribed,
    *  send an update RPC, but the server sees the unsubscription RPC first.
    *  When the server sees the "new" client update request, the versions will be
    *  out of date.
    */

	args := UnsubscribeArgs{User: u.me}
	reply := UnsubscribeReply{}
	ok := u.app.Call("AppServer.UnsubscribeHandler", &args, &reply)
	if !ok {
		return ErrDisconnect
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.subscribed == 0 && reply.Err == OK {
		u.serviceKnownVersion = 0
	}
	return reply.Err
}

func (u *User) Pause() Err {
	u.DPrintf("PAUSING!")
	atomic.StoreInt32(&u.subscribed, 0)
	return OK
}

func (u *User) Unpause() Err {
	u.DPrintf("UNPAUSING!")
	atomic.StoreInt32(&u.subscribed, 1)
	return OK
}

func (u *User) statusOK() bool {
	return !u.killed() && u.is_subscribed()
}

func (u *User) AddArticle(content string) (int64, Err) {
	if !u.statusOK() {
		return -1, ErrDisconnect
	}
	u.mu.Lock()
	u.currentVersion += 1
	newVersion := u.currentVersion
	u.mu.Unlock()
	newText := Text{
		User:    u.me, 
		Id:      rand.Int63(),
		Parent:  ArticleParentId,
		Content: content,
		Version: newVersion,
	}

	args := UpdateArgs{
		User:     u.me,
		Texts:    []Text{newText},
		Votes:    []Vote{},
		IsDelete: false,
		IsUpload: false,
	}

	_ = u.logger.SaveLogEntry(&args)
	err := u.SendAppServerUpdate(&args)
	if err == OK && u.statusOK() {
		if dberr := u.InsertTextRecordsDB(newText); dberr != nil {
			err = ErrDB
		}
	}

	// update that this log entry has committed. but what if the prior entry hasn't?
	// need some way for a thread to go through and "commit" entries...
	// one way to do this is to keep a slice of committed versions,
	// and then have a thread occasionally go and update the logcheckpt
	//u.mu.Lock()
	//if u.logCheckpt < offset {
	//u.logCheckpt = offset
	//}
	//u.mu.Unlock()

	return newText.Id, err
}

func (u *User) AddComment(content string, parentId int64) (int64, Err) {
	if !u.statusOK() {
		return -1, ErrDisconnect
	}
	u.mu.Lock()
	u.currentVersion += 1
	newVersion := u.currentVersion
	u.mu.Unlock()
	newText := Text{
		User:    u.me, 
		Id:      rand.Int63(),
		Parent:  parentId,
		Content: content,
		Version: newVersion,
	}

	args := UpdateArgs{
		User:     u.me,
		Texts:    []Text{newText},
		Votes:    []Vote{},
		IsDelete: false,
		IsUpload: false,
	}

	_ = u.logger.SaveLogEntry(&args)
	err := u.SendAppServerUpdate(&args)
	if err == OK && u.statusOK() {
		if dberr := u.InsertTextRecordsDB(newText); dberr != nil {
			err = ErrDB
		}
	}

	// update that this log entry has committed. but what if the prior entry hasn't?
	// need some way for a thread to go through and "commit" entries...
	// one way to do this is to keep a slice of committed versions,
	// and then have a thread occasionally go and update the logcheckpt
	//u.mu.Lock()
	//if u.logCheckpt < offset {
	//u.logCheckpt = offset
	//}
	//u.mu.Unlock()
	return newText.Id, err
}

func (u *User) UpdateText(id int64, content string) Err {
	if !u.statusOK() {
		return ErrDisconnect
	}

	u.mu.Lock()
	u.currentVersion += 1
	newText := Text{
		User:    u.me, 
		Id:      id,
		Content: content,
		Version: u.currentVersion,
	}
	u.mu.Unlock()

	args := UpdateArgs{
		User:     u.me,
		Texts:    []Text{newText},
		Votes:    []Vote{},
		IsDelete: false,
		IsUpload: false,
	}

	_ = u.logger.SaveLogEntry(&args)
	err := u.SendAppServerUpdate(&args)
	if err == OK && u.statusOK() {
		if dberr := u.UpdateTextRecordsDB(newText); dberr != nil {
			err = ErrDB
		}
	}
	return err
}

func (u *User) AddVote(id int64, isPos bool) Err {
	if !u.statusOK() {
		return ErrDisconnect
	}
	u.mu.Lock()
	u.currentVersion += 1
	newVote := Vote{
		User:    u.me, 
		Id:      id,
		IsPos:   isPos,
		Version: u.currentVersion,
	}
	u.mu.Unlock()

	args := UpdateArgs{
		User:     u.me,
		Votes:    []Vote{newVote},
		Texts:    []Text{},
		IsDelete: false,
		IsUpload: false,
	}

	_ = u.logger.SaveLogEntry(&args)
	// insert int DB afterward so that
	// it won't be saved in the database, but not
	// appear in the service data if there was an error
	err := u.SendAppServerUpdate(&args)
	if err == OK && u.statusOK() {
		if dberr := u.InsertVoteDB(newVote); dberr != nil {
			err = ErrDB
		}
	}
	return err
}

func (u *User) UpdateVote(id int64, isPos bool) Err {
	if !u.statusOK() {
		return ErrDisconnect
	}
	u.mu.Lock()
	u.currentVersion += 1
	newVote := Vote{
		User:    u.me, 
		Id:      id,
		IsPos:   isPos,
		Version: u.currentVersion,
	}
	u.mu.Unlock()

	args := UpdateArgs{
		User:     u.me,
		Votes:    []Vote{newVote},
		Texts:    []Text{},
		IsDelete: false,
		IsUpload: false,
	}

	_ = u.logger.SaveLogEntry(&args)
	err := u.SendAppServerUpdate(&args)
	if err == OK && u.statusOK() {
		if dberr := u.UpdateVoteDB(newVote); dberr != nil {
			err = ErrDB
		}
	}
	return err
}

/* DELETION */
func (u *User) DeleteVote(id int64) Err {
	if !u.statusOK() {
		return ErrDisconnect
	}

	u.mu.Lock()
	u.currentVersion += 1
	newVersion := u.currentVersion
	u.mu.Unlock()
	toDelete := Vote{Id: id, Version: newVersion}
	args := UpdateArgs{
		User:     u.me,
		Votes:    []Vote{toDelete},
		Texts:    []Text{},
		IsDelete: true,
		IsUpload: false,
	}
	_ = u.logger.SaveLogEntry(&args)
	err := u.SendAppServerUpdate(&args)
	if err == OK && u.statusOK() {
		if dberr := u.DeleteVoteDB(id); dberr != nil {
			err = ErrDB
		}
	}
	return err
}

func (u *User) DeleteText(id int64) Err {
	if !u.statusOK() {
		return ErrDisconnect
	}

	u.mu.Lock()
	u.currentVersion += 1
	newVersion := u.currentVersion
	u.mu.Unlock()
    toDelete := Text{
        User: u.me, 
        Id: id, 
        Version: newVersion,
    }
	args := UpdateArgs{
		User:     u.me,
		Votes:    []Vote{},
		Texts:    []Text{toDelete},
		IsDelete: true,
		IsUpload: false,
	}
	_ = u.logger.SaveLogEntry(&args)
	err := u.SendAppServerUpdate(&args)
	if err == OK && u.statusOK() {
		if dberr := u.DeleteTextRecordsDB(id); dberr != nil {
			err = ErrDB
		}
	}
	return err
}

/* READ */
func (u *User) Read(id int64) *ReadReply {
	if !u.statusOK() {
		return &ReadReply{Err: ErrDisconnect}
	}

	args := ReadArgs{
		User: u.me,
		Id:   id,
	}
	reply := &ReadReply{}
	ok := u.app.Call("AppServer.ReadHandler", &args, reply)
	if !ok {
		//log.Fatalf("Read: Could not connect to server")
		reply.Err = ErrDisconnect
		return reply
	}
	u.mu.Lock()
	u.serviceKnownVersion = max(u.serviceKnownVersion, reply.KnownVersion)
	//checkVer := u.serviceKnownVersion
	//curVer := u.currentVersion
	u.mu.Unlock()
	/*if checkVer < curVer && u.statusOK() {
		u.DPrintf("Calling update server, version %d vs sver %d", curVer, checkVer)
		u.UploadSinceVersion(reply.KnownVersion)
	}*/
	return reply
}

func (u *User) SendAppServerUpdate(args *UpdateArgs) Err {
	u.DPrintf("Send update %d texts %d votes", len(args.Texts), len(args.Votes))
	reply := UpdateReply{}
	ok := u.app.Call("AppServer.UpdateHandler", args, &reply)
	if !ok {
		return ErrDisconnect
	}
	u.mu.Lock()
	u.serviceKnownVersion = max(u.serviceKnownVersion, reply.KnownVersion)
	//checkVer := u.serviceKnownVersion
	//curVer := u.currentVersion
	u.mu.Unlock()
	/*if checkVer < curVer && u.statusOK() {
		u.DPrintf("Calling update server, version %d vs sver %d", curVer, checkVer)
		u.UploadSinceVersion(reply.KnownVersion)
	}*/
	return reply.Err
}

func (u *User) is_subscribed() bool {
	z := atomic.LoadInt32(&u.subscribed)
	return z == 1
}

func (u *User) Kill() {
	u.DPrintf("KILLING!")
	atomic.StoreInt32(&u.dead, 1)
	u.db.Close()
	u.logger.Close()
}

func (u *User) killed() bool {
	z := atomic.LoadInt32(&u.dead)
	return z == 1
}

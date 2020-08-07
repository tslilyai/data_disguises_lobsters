package gdpr

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Id2TextMap map[int64]Text
type Id2VoteMap map[int64]Vote
type Id2BoolMap map[int64]bool

const (
	ArticleParentId = -1
	leaseS          = 157680000 // 5 years
)

type AppServer struct {
	mu       sync.Mutex
	me       int
	dead     int32 // set by Kill()
	policies EffectsPolicies
	key      []byte
	block    cipher.Block

	// Mapping from Ids to content
	Id2Texts      map[int64]Text
	Id2VoteCounts map[int64]VoteCounts
	// For "revoke" policy
	InvisId2InvisTexts map[string]InvisText
	InvisId2InvisIsPos map[string]bool

	// Mapping from users to user contents
	U2Ids   map[int]Id2BoolMap // copy of IDs owned by each user
	U2Votes map[int]Id2VoteMap // copy of votes of each user

	// Information pertinent to each user
	U2Version map[int]int64     // last updated version of each user
	U2Time    map[int]time.Time // last time each user heard from

	// STATS
	// TODO
	NumWrites int // number of writes performed
}

func StartAppServer(me int, policies EffectsPolicies) *AppServer {
	as := new(AppServer)
	as.me = me
	as.policies = policies

	as.Id2Texts = make(map[int64]Text)
	as.Id2VoteCounts = make(map[int64]VoteCounts)

	as.U2Ids = make(map[int]Id2BoolMap)
	as.U2Votes = make(map[int]Id2VoteMap)
	as.U2Version = make(map[int]int64)
	as.U2Time = make(map[int]time.Time)
	as.NumWrites = 0

	as.InvisId2InvisTexts = make(map[string]InvisText)
	as.InvisId2InvisIsPos = make(map[string]bool)
	as.key, _ = hex.DecodeString("6368616e676520746869732070617373776f726420746f206120736563726574")
	var err error
	as.block, err = aes.NewCipher(as.key)
	if err != nil {
		panic(err.Error())
	}

	// thread that checks to see if user leases are up
	go func() {
		if as.killed() {
			return
		}
		for {
			as.mu.Lock()
			for user, t := range as.U2Time {
				if time.Now().Sub(t).Seconds() >= leaseS {
					as.unsubscribeUser(user)
				}
			}
			as.mu.Unlock()
			time.Sleep(10 * time.Second)
		}
	}()

	as.DPrintf("App server starting!")
	return as
}

func (as *AppServer) encrypt(ID int64, user int) string {
	// TODO make secret, randomly generated + stored by appserver
	idb := make([]byte, 8)
	userb := make([]byte, 8)
	binary.LittleEndian.PutUint64(idb, uint64(ID))
	binary.LittleEndian.PutUint64(userb, uint64(user))
	bytes := append(idb, userb...)

	aesgcm, err := cipher.NewGCM(as.block)
	if err != nil {
		panic(err.Error())
	}

	// for now, just use the bytes as the nonce
	ciphertext := aesgcm.Seal(nil, bytes[:12], bytes, nil)
	return string(ciphertext)
}

/* Must be locked */
func (as *AppServer) addUser(user int) {
	found := false
	// add users if don't exist
	if _, found = as.U2Ids[user]; !found {
		as.U2Ids[user] = make(map[int64]bool)
	}
	if _, found = as.U2Votes[user]; !found {
		as.U2Votes[user] = make(map[int64]Vote)
	}
	if _, found = as.U2Version[user]; !found {
		as.U2Version[user] = -1
	}
	as.U2Time[user] = time.Now()
}

func (as *AppServer) UpdateHandler(args *UpdateArgs, reply *UpdateReply) {
	// assume ok unless otherwise
	reply.Err = OK

	as.mu.Lock()
	defer as.mu.Unlock()

	// set initial version to return
	as.addUser(args.User)

	if args.IsDelete {
		reply.Err = as.deleteTextRecords(args.Texts)
		if reply.Err == OK {
			reply.Err = as.deleteVoteRecords(args.Votes)
		}
	} else {
		reply.Err = as.updateTextRecords(args.Texts, args.IsUpload)
		if reply.Err == OK {
			reply.Err = as.updateVoteRecords(args.Votes, args.IsUpload)
		}
	}
	reply.KnownVersion = as.U2Version[args.User]
}

func (as *AppServer) UnsubscribeHandler(args *UnsubscribeArgs, reply *UpdateReply) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.unsubscribeUser(args.User)
	// XXX detect if there somehow was a mismatch in in-memory tables?
	reply.Err = OK
	return
}

/* must be locked */
func (as *AppServer) unsubscribeUser(user int) {
	as.DPrintf("%d: Unsubscribe called", user)

	textsToDelete := []Text{}
	for id, _ := range as.U2Ids[user] {
		textsToDelete = append(textsToDelete, as.Id2Texts[id])
	}
	votesToDelete := []Vote{}
	for _, v := range as.U2Votes[user] {
		as.DPrintf("Adding vote %d to delete!", v.Id)
		votesToDelete = append(votesToDelete, v)
	}

	// delete texts
	err := as.deleteTextRecords(textsToDelete)
	if err != OK && err != ErrNotFound {
		log.Printf("Couldn't delete texts? %d", err)
	}
	// delete votes
	err = as.deleteVoteRecords(votesToDelete)
	if err != OK && err != ErrNotFound {
		//XXX TODO find out why duplicates?
		log.Printf("Couldn't delete votes? %d", err)
	}

	delete(as.U2Votes, user)
	delete(as.U2Ids, user)
	delete(as.U2Version, user)
	delete(as.U2Time, user)
}

func (as *AppServer) ReadHandler(args *ReadArgs, reply *ReadReply) {
	as.DPrintf("%d: Read called", args.User)
	as.mu.Lock()
	defer as.mu.Unlock()

	as.addUser(args.User)
	// if the text doesn't exist, return
	a, found := as.Id2Texts[args.Id]
	if !found {
		reply.Err = ErrNotFound
		return
	}
	v, found := as.Id2VoteCounts[args.Id]
	if !found {
		reply.Err = ErrNotFound
		return
	}
	reply.KnownVersion = as.U2Version[args.User]
	reply.Text = a
	reply.VoteCounts = v
	reply.Err = OK
}

/*
 call Kill() when a AppServer instance won't be needed again.
*/
func (as *AppServer) Kill() {
	atomic.StoreInt32(&as.dead, 1)
}

func (as *AppServer) killed() bool {
	z := atomic.LoadInt32(&as.dead)
	return z == 1
}

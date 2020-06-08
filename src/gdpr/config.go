package gdpr

import (
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"../labrpc"

	crand "crypto/rand"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"math/big"
	"time"
    //"gonum.org/v1/gonum/stat"
)

const numArticles int = 198965


func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (cfg *config) InitConfigIds(numUsers int, dataFolder string) {
	var wg sync.WaitGroup
	wg.Add(numUsers)

	cfg.ids = nil
	for u := 0; u < numUsers; u++ {
		cfg.ids = append(cfg.ids, make(map[int64]bool))
	}
	for u := 0; u < numUsers; u++ {
		go func(u int) {
			filename := fmt.Sprintf(dataFolder+"/%d/TextRecord.csv", u)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Could not open textrecord csv: " + err.Error())
			}
			rows, err := csv.NewReader(file).ReadAll()
			file.Close()
			if err != nil {
				log.Fatalf("Could not read textrecord csv: " + err.Error())
			}
			for _, r := range rows {
				id, err := strconv.Atoi(r[0])
				if err != nil {
					log.Fatalf("Could not convert id in csv: " + err.Error())
				}
				cfg.ids[u][int64(id)] = true
			}
			log.Printf("Initialized ids for user %d", u)
			wg.Done()
		}(u)
	}
	wg.Wait()
}

type config struct {
	mus              []sync.Mutex
	t                *testing.T
	net              *labrpc.Network
	nusers           int
	users            []*User
	updateFreq       []int64
	appserver        *AppServer
	serviceName      string
	serviceConnected bool                // whether service is on the net
	connected        []bool              // whether each server is on the net
	ends             []*labrpc.ClientEnd // outgoing ends to all users
	endnames         []string            // outgoing endnames to all users
	logs             []*Logger
	dead             int32 // set by Kill()
    ids             []map[int64]bool
	/* stats */
	readTimes   [][]float64
	updateTimes [][]float64
	deleteTimes [][]float64

	// begin()/end() statistics
	t0        time.Time // time at which test_test.go called cfg.begin()
	rpcs0     int       // rpcTotal() at start of test
	cmds0     int       // number of agreements
	bytes0    int64
	maxIndex  int
	maxIndex0 int
}

var ncpu_once sync.Once

func make_config(t *testing.T, nusers int, serviceName string, unreliable bool, policy EffectsPolicies) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.nusers = nusers
	cfg.mus = make([]sync.Mutex, cfg.nusers)
	cfg.users = make([]*User, cfg.nusers)
	cfg.updateFreq = make([]int64, cfg.nusers)
	cfg.connected = make([]bool, cfg.nusers)
	cfg.ends = make([]*labrpc.ClientEnd, cfg.nusers)
	cfg.endnames = make([]string, cfg.nusers)

	for i := 0; i < cfg.nusers; i++ {
		cfg.readTimes = append(cfg.readTimes, []float64{})
		cfg.updateTimes = append(cfg.updateTimes, []float64{})
		cfg.deleteTimes = append(cfg.deleteTimes, []float64{})
	}
	cfg.logs = make([]*Logger, cfg.nusers)
	cfg.setunreliable(unreliable)
	cfg.net.LongDelays(false)
	cfg.serviceName = serviceName

	// start the app server with the provided endnames
	cfg.appserver = StartAppServer(0, policy)

	// enable the appserver
	svc := labrpc.MakeService(cfg.appserver)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(cfg.serviceName, srv)
	cfg.connectService()

	var wg sync.WaitGroup
	wg.Add(cfg.nusers)

	// create a full set of users.
	for i := 0; i < cfg.nusers; i++ {
		go func(i int) {
			cfg.crash1(i)
			logName := fmt.Sprintf("log_%d", i)
			os.Remove(logName)
			cfg.logs[i] = MakeLogger(logName)
			cfg.endnames[i] = randstring(20)
			cfg.ends[i] = cfg.net.MakeEnd(cfg.endnames[i])
			cfg.net.Connect(cfg.endnames[i], cfg.serviceName)

			u := MakeUser(i, cfg.logs[i], cfg.ends[i])
			cfg.mus[i].Lock()
			cfg.users[i] = u
			cfg.mus[i].Unlock()

			svc := labrpc.MakeService(u)
			srv := labrpc.MakeServer()
			srv.AddService(svc)
			cfg.net.AddServer(i, srv)
			cfg.connected[i] = true
			cfg.net.Enable(cfg.endnames[i], true)

			u.Start()
			log.Printf("User %d started!\n", i)
			wg.Done()
		}(i)
	}

	wg.Wait()
	return cfg
}

// shut down a user server but save its persistent state.
func (cfg *config) crash1(i int) {
	//log.Printf("crashing user %d\n", i)
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // disable client connections to the server.

	cfg.mus[i].Lock()
	defer cfg.mus[i].Unlock()

	u := cfg.users[i]
	if u != nil {
		u.Kill()
		cfg.users[i] = nil
	}
}

func (cfg *config) runUser(user int, minUpdateMs int, maxUpdateMs int, wg *sync.WaitGroup) {
	sleepTime := 10 * time.Millisecond
	cfg.updateFreq[user] = int64(minUpdateMs) // int64(rand.Intn(maxUpdateMs-minUpdateMs+1) + minUpdateMs)

	wg.Done()
	wg.Wait()
	for !cfg.killed() {
		start := time.Now()
		time.Sleep(sleepTime)
		for time.Now().Sub(start).Milliseconds() < cfg.updateFreq[user] {
			time.Sleep(sleepTime)
		}
		if cfg.users[user].killed() {
			return
		}

		action := rand.Intn(10)
		uIndex := int(rand.Intn(cfg.nusers))
		var myId, otherId int64
		if len(cfg.ids[user]) == 0 {
			action = 0
		} else if len(cfg.ids[uIndex]) == 0 {
			cfg.mus[user].Lock()
			myId = getRandomKey(cfg.ids[user])
			cfg.mus[user].Unlock()
			otherId = myId
		} else {
			cfg.mus[user].Lock()
			myId = getRandomKey(cfg.ids[user])
			cfg.mus[user].Unlock()
			cfg.mus[uIndex].Lock()
			otherId = getRandomKey(cfg.ids[uIndex])
			cfg.mus[uIndex].Unlock()
		}

        // only do vote updates for now
        action = 1

		t0 := time.Now()
		switch {
		case action == 1: // add text (comment)
			id, err := cfg.users[user].AddComment(fmt.Sprintf("hello world %d:%d", user, rand.Intn(math.MaxInt64)), otherId)
			if err != OK {
				DPrintf("runUser %d: unknown add comment err %d", user, err)
			} else {
			    cfg.mus[user].Lock()
			    cfg.ids[user][id] = true
                testTime := float64((time.Now().Sub(t0)).Microseconds())
			    cfg.updateTimes[user] = append(cfg.updateTimes[user], testTime)
			    DPrintf("User %d: add comment %d\n", user, id)
			    cfg.mus[user].Unlock()
			}
			break
		case action == 2: // add (or update) vote
			err := cfg.users[user].AddVote(otherId, rand.Intn(2) == 1)
			if err != OK && err != ErrNotFound {
				DPrintf("runUser %d: unknown add vote %d err %d", user, otherId, err)
			} else {
			    DPrintf("User %d: update vote %d\n", user, otherId)
			    cfg.mus[user].Lock()
                testTime := float64((time.Now().Sub(t0)).Microseconds())
			    cfg.updateTimes[user] = append(cfg.updateTimes[user], testTime)
			    cfg.mus[user].Unlock()
			}
			break
		case action == 3: // delete text
			err := cfg.users[user].DeleteText(myId)
			if err != OK {
				DPrintf("runUser %d: unknown delete %d err %d", user, myId, err)
			} else {
			    DPrintf("User %d, delete article %d\n", user, myId)
			    cfg.mus[user].Lock()
			    cfg.deleteTimes[user] = append(cfg.deleteTimes[user], float64((time.Now().Sub(t0)).Microseconds()))
			    cfg.mus[user].Unlock()
			}
			break
		case action == 4: // read
			readRep := cfg.users[user].Read(otherId)
			if readRep.Err != OK {
				DPrintf("runUser %d: unknown read %d err %d", user, otherId, readRep.Err)
			} else {
			    DPrintf("User %d, read article %d\n", user, myId)
			    cfg.mus[user].Lock()
                testTime := float64((time.Now().Sub(t0)).Microseconds())
			    cfg.readTimes[user] = append(cfg.readTimes[user], testTime)
			    cfg.mus[user].Unlock()
			}
			break
		default:
			break
		}
	}
}

func (cfg *config) printStats(fup, fdel, fread *os.File) {
	updatetimes := ""
	deletetimes := ""
	readtimes := ""
	for _, timeSlice := range cfg.updateTimes {
        //mean, variance := stat.MeanVariance(timeSlice, nil)
		updatetimes += fmt.Sprintf("%v\n", timeSlice)//, mean, variance)
		//updatetimes += fmt.Sprintf("%v, %v, %v\n", timeSlice, mean, variance)
	}
    for _, timeSlice := range cfg.readTimes {
		//mean, variance := stat.MeanVariance(timeSlice, nil)
		readtimes += fmt.Sprintf("%v\n", timeSlice)
		//readtimes += fmt.Sprintf("%v, %v\n", mean, variance)
	}
    for _, timeSlice := range cfg.deleteTimes {
		//mean, variance := stat.MeanVariance(timeSlice, nil)
		deletetimes += fmt.Sprintf("%v\n", timeSlice)
		//deletetimes += fmt.Sprintf("%v, %v\n", mean, variance)
	}
	fup.WriteString(updatetimes + "\n")
	fdel.WriteString(deletetimes + "\n")
	fread.WriteString(readtimes + "\n")
}

func (cfg *config) cleanup() {
	cfg.Kill()
	cfg.appserver.Kill()
	for i := 0; i < len(cfg.users); i++ {
		if cfg.users[i] != nil {
			cfg.users[i].Kill()
		}
	}
	cfg.net.Cleanup()
}

// detach server i from the net.
func (cfg *config) connectService() {
	//fmt.Printf("connect service\n")
	cfg.serviceConnected = true
	cfg.net.Enable(cfg.serviceName, true)
}

// detach server i from the net.
func (cfg *config) disconnectService() {
	//fmt.Printf("disconnect service\n")
	cfg.serviceConnected = false
	cfg.net.Enable(cfg.serviceName, false)
}

// detach server i from the net.
func (cfg *config) disconnect(i int) {
	//fmt.Printf("disconnect(%d)\n", i)
	cfg.connected[i] = false
	cfg.net.Enable(cfg.endnames[i], false)
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) bytesTotal() int64 {
	return cfg.net.GetTotalBytes()
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

// start a Test.
// print the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	//fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()
	cfg.cmds0 = 0
	cfg.maxIndex0 = cfg.maxIndex
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	if cfg.t.Failed() == false {
		t := time.Since(cfg.t0).Seconds()       // real time
		npeers := cfg.nusers                    // number of user peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0      // number of RPC sends
		nbytes := cfg.bytesTotal() - cfg.bytes0 // number of bytes
		ncmds := cfg.maxIndex - cfg.maxIndex0   // number of user agreements reported

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, npeers, nrpc, nbytes, ncmds)
	}
}

/*
 call Kill() when a Config instance won't be needed again.
*/
func (as *config) Kill() {
	atomic.StoreInt32(&as.dead, 1)
}

func (as *config) killed() bool {
	z := atomic.LoadInt32(&as.dead)
	return z == 1
}

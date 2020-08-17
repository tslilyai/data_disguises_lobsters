package gdpr

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNormalExecution(t *testing.T) {
	fmt.Printf("TestNormalExecution\n")
	nUsers := []int{5}
	nTrials := 2
	testDuration := 20 * time.Second
	minUpdateMs := 10
	maxUpdateMs := 100

	policyRet := EffectsPolicies{
		VotePolicy: Retain,
		TextPolicy: Retain,
	}

	policyRev := EffectsPolicies{
		VotePolicy: Revoke,
		TextPolicy: Revoke, 
	}

	policyRevDel := EffectsPolicies{
		VotePolicy: RevokeDelete,
		TextPolicy: RevokeDelete,
	}

	for _, n := range nUsers {
		for _, policy := range []EffectsPolicies{policyRev, policyRevDel, policyRet} {
		    for _, baseline := range []bool{true,false} {
                suffix := "test_"
                if baseline {
                    suffix = "baseline_"
                }
                suffix += fmt.Sprintf("%s", PoliciesToString(policy))
                fsubs, err := os.OpenFile("subscribes_"+suffix+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    log.Fatal(err)
                }
                fup, err := os.OpenFile("updates_"+suffix+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    log.Fatal(err)
                }
                fdel, err := os.OpenFile("deletes_"+suffix+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    log.Fatal(err)
                }
                fread, err := os.OpenFile("reads_"+suffix+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    log.Fatal(err)
                }
                defer fup.Close()
                defer fdel.Close()
                defer fread.Close()
                defer fsubs.Close()

                for trial := 0; trial < nTrials; trial++ {
                    fmt.Printf("Making config\n")
                    cfg := make_config(t, n, "articles_service", false /*is_unreliable*/, policy)
                    cfg.InitConfigIds(n, "data")
                    cfg.begin("test")

                    var wg sync.WaitGroup
                    wg.Add(1)
                    fmt.Printf("Running users\n")
                    for i := 0; i < 1; i++ {
                        go func(i int) {
                            cfg.runUser(i, minUpdateMs, maxUpdateMs, &wg)
                        }(i)
                    }
                    wg.Wait()

                    start := time.Now()
                    now := start
                    for now.Sub(start) < testDuration {
                        time.Sleep(10*time.Second)
                        now = time.Now()
                    }
                    cfg.printStats(fup, fdel, fread)
                    cfg.cleanup()
                }
            }
        }
    }
}

func TestSubscribeUnsubscribe(t *testing.T) {
	fmt.Printf("TestSubscribeUnsubscribe\n")
	nUsers := []int{1}
	nTrials := 1
	testDuration := 40 * time.Second
	minUpdateMs := 50
	maxUpdateMs := 100
	subscribeInterval := 10 * time.Second
	subscribed := true

	policyRet := EffectsPolicies{
		VotePolicy: Retain,
		TextPolicy: Retain,
	}

	policyRev := EffectsPolicies{
		VotePolicy: Revoke,
		TextPolicy: Revoke, 
	}

	policyRevDel := EffectsPolicies{
		VotePolicy: RevokeDelete,
		TextPolicy: RevokeDelete,
	}

	for _, n := range nUsers {
		for _, policy := range []EffectsPolicies{policyRev, policyRevDel, policyRet} {
		    for _, baseline := range []bool{true,false} {
                suffix := "test_"
                if baseline {
                    suffix = "baseline_"
                }
                suffix += fmt.Sprintf("%s", PoliciesToString(policy))
                fsubs, err := os.OpenFile("subscribes_"+suffix+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    log.Fatal(err)
                }
                fup, err := os.OpenFile("updates_"+suffix+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    log.Fatal(err)
                }
                fdel, err := os.OpenFile("deletes_"+suffix+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    log.Fatal(err)
                }
                fread, err := os.OpenFile("reads_"+suffix+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                if err != nil {
                    log.Fatal(err)
                }
                defer fup.Close()
                defer fdel.Close()
                defer fread.Close()
                defer fsubs.Close()

                for trial := 0; trial < nTrials; trial++ {
                    fmt.Printf("Making config\n")
                    cfg := make_config(t, n, "articles_service", false /*is_unreliable*/, policy)
                    cfg.InitConfigIds(n, "data")
                    cfg.begin("test")

                    var wg sync.WaitGroup
                    wg.Add(n)
                    fmt.Printf("Running users\n")
                    for i := 0; i < n; i++ {
                        go func(i int) {
                            cfg.runUser(i, minUpdateMs, maxUpdateMs, &wg)
                        }(i)
                    }
                    subscribed = true
                    wg.Wait()

                    DPrintf("Starting new test\n")
                    start := time.Now()
                    now := start
                    for now.Sub(start) < testDuration {
                        if now.Sub(start) > subscribeInterval {
                            if subscribed {
                                fsubs.WriteString("Unsubscribe: ")
                                if baseline {
                                    cfg.users[0].Pause()
                                } else {
                                    cfg.users[0].Unsubscribe()
                                }
                                fsubs.WriteString(fmt.Sprintf("%v\n", time.Now().Sub(now).Milliseconds()))
                            } else {
                                fsubs.WriteString("Subscribe: ")
                                if baseline {
                                    cfg.users[0].Unpause()
                                } else {
                                    cfg.users[0].Subscribe()
                                }
                                fsubs.WriteString(fmt.Sprintf("%v\n", time.Now().Sub(now).Milliseconds()))
                            }
                            subscribed = !subscribed
                            time.Sleep(5 * time.Second)
                        }
                        now = time.Now()
                    }
                    cfg.printStats(fup, fdel, fread)
                    cfg.cleanup()
                }
            }
        }
    }
}
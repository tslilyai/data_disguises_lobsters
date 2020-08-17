package gdpr

import (
	"fmt"
	"log"
	"math/rand"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func (as *AppServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 && !as.killed() {
		header := fmt.Sprintf("SERVER\t#%d\t", as.me)
		log.Printf(header+format, a...)
	}
	return
}

func (c *User) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 && !c.killed() {
		header := fmt.Sprintf("USER\t#%d\t", c.me)
		log.Printf(header+format, a...)
	}
	return
}

// max aid number per user
func getRandomKey(m map[int64]bool) int64 {
	i := rand.Intn(len(m))
	for k := range m {
		if i == 0 {
			return k
		}
		i--
	}
	return -1
}

func strmax(a, b string) string {
	if a > b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxint(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func findAndRemoveVal(s []int64, v int64) []int64 {
	for i := 0; i < len(s); i++ {
		if s[i] == v {
			s[i] = s[len(s)-1]
			return s[:len(s)-1]
		}
	}
	return s
}

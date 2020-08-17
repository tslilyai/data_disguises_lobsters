package gdpr

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"sync"

	"../labgob"
)

type Logger struct {
	mu    sync.Mutex
	name  string
	logFH *os.File
}

func MakeLogger(logfile string) *Logger {
	var err error
	l := &Logger{}
	l.name = logfile
	l.logFH, err = os.OpenFile(logfile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Could not open file: %s", err)
	}
	return l
}

func (l *Logger) Close() {
	l.logFH.Close()
}

func (l *Logger) SaveLogEntry(update *UpdateArgs) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	// always write at the end of the file
	if _, err := l.logFH.Seek(0, 2); err != nil {
		return -1
		log.Fatal(err.Error())
	}

	// encode the update
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(update)
	bytes := w.Bytes()

	// write the len of the bytes to read
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(len(bytes)))
	n, err := l.logFH.Write(buf)
	if err != nil || n != len(buf) {
		return -1
		log.Fatal(err.Error())
	}
	n, err = l.logFH.Write(bytes)
	if err != nil || n != len(bytes) {
		return -1
		log.Fatal(err.Error())
	}

	// sync XXX batch?
	l.logFH.Sync()

	// get the position of the FH to return to user
	offset, err := l.logFH.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1
		log.Fatal(err)
	}
	return offset
}

func (l *Logger) ReadLogEntriesSince(lastCommittedEntryPos int64) []UpdateArgs {
	l.mu.Lock()
	defer l.mu.Unlock()

	// start from beginning of log
	if _, err := l.logFH.Seek(0, 0); err != nil {
		return []UpdateArgs{}
		log.Fatal(err)
	}

	entries := []UpdateArgs{}
	for {
		// decode size of entry
		sizebuf := make([]byte, binary.MaxVarintLen64)
		count, err := l.logFH.Read(sizebuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return []UpdateArgs{}
			log.Fatal(err)
		}
		if count != binary.MaxVarintLen64 {
			return []UpdateArgs{}
			log.Fatal("could not read size of entry")
		}
		size, count := binary.Varint(sizebuf)

		// decode entry
		entrybuf := make([]byte, size)
		count, err = l.logFH.Read(entrybuf)
		if err != nil {
			return []UpdateArgs{}
			log.Fatal(err)
		}
		if int64(count) != size {
			break
		}
		r := bytes.NewBuffer(entrybuf)
		d := labgob.NewDecoder(r)

		var updateEntry UpdateArgs
		if d.Decode(&updateEntry) != nil {
			// XXX do we need checksums?
			return []UpdateArgs{}
			log.Fatal("decode entry error")
		}
		entries = append(entries, updateEntry)
	}
	return entries
}

package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

var prof struct {
	cpu, mem *os.File
}

func formatKey(key []byte, buf []byte) []byte {
	needed := len(key) + 16
	if cap(buf) < needed {
		buf = make([]byte, needed)
	} else {
		buf = buf[:needed]
	}
	hex.Encode(buf[:32], key[:16])
	copy(buf[32:], key[16:])
	return buf[:needed]
}

func main() {
	var (
		cpuprofile, memprofile string
		orgId                  idFlag
		bucketId               idFlag
	)

	flag.StringVar(&cpuprofile, "cpuprofile", "", "capture CPU profile")
	flag.StringVar(&memprofile, "memprofile", "", "capture memory profile")
	flag.Var(&orgId, "org-id", "8-byte (hex) org id")
	flag.Var(&bucketId, "bucket-id", "8-byte (hex) bucket id")
	flag.Parse()

	filename := flag.Arg(0)
	if filename == "" {
		fmt.Println("no file")
		os.Exit(1)
	}

	f, err := os.Open(filename)
	if err != nil {
		fmt.Println("error opening file: ", err)
		os.Exit(1)
	}

	startProfile(cpuprofile, memprofile)
	defer stopProfile()

	var ts = tsm1.NewTombstoner(filename, nil)
	var buf []byte
	ts.Walk(func(t tsm1.Tombstone) error {
		if len(orgId) == 8 && !bytes.Equal(orgId, t.Key[:8]) {
			return nil
		}
		if len(bucketId) == 8 && !bytes.Equal(bucketId, t.Key[8:16]) {
			return nil
		}

		buf = formatKey(t.Key, buf)
		fmt.Printf("%s\t%s\t%s\t%s\t%s\n", time.Unix(0, t.Min).Format(time.RFC3339Nano), time.Unix(0, t.Max).Format(time.RFC3339Nano), string(buf[:16]), string(buf[16:32]), string(buf[33:]))
		return nil
	})

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		f.Close()
		fmt.Println("reader error: ", err)
		os.Exit(1)
	}
	defer r.Close()
}

// StartProfile initializes the cpu and memory profile, if specified.
func startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		log.Printf("writing mem profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profile stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("mem profile stopped")
	}
}

type idFlag []byte

func (hv idFlag) String() string {
	return hex.EncodeToString(hv)
}

func (hv *idFlag) Set(v string) (err error) {
	*hv, err = hex.DecodeString(v)
	if err != nil {
		return err
	}
	if len(*hv) != 8 {
		err = errors.New("invalid ID length; must be 8 bytes")
	}
	return err
}

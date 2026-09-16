// Command server starts the mini-redis-go TCP server.
package main

import (
	"flag"
	"log"

	"github.com/Breyes05/mini-redis-go/internal/persistence"
	"github.com/Breyes05/mini-redis-go/internal/server"
	"github.com/Breyes05/mini-redis-go/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "address to listen on")
	aofPath := flag.String("aof", "appendonly.aof", "append-only file path (empty disables persistence)")
	fsync := flag.String("fsync", "everysec", "fsync policy for the AOF: 'always' or 'everysec'")
	flag.Parse()

	st := store.New()
	defer st.Close()

	srv := server.New(*addr, st)

	if *aofPath != "" {
		log.Printf("replaying %s", *aofPath)
		if err := persistence.Replay(*aofPath, srv.Apply); err != nil {
			log.Fatalf("replay aof: %v", err)
		}

		policy := persistence.FsyncEverySec
		if *fsync == "always" {
			policy = persistence.FsyncAlways
		}
		aof, err := persistence.Open(*aofPath, policy)
		if err != nil {
			log.Fatalf("open aof: %v", err)
		}
		defer aof.Close()
		srv.SetAOF(aof)
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

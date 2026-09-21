// Command server starts the mini-redis-go TCP server.
package main

import (
	"flag"
	"log"

	"github.com/Breyes05/mini-redis-go/internal/persistence"
	"github.com/Breyes05/mini-redis-go/internal/replication"
	"github.com/Breyes05/mini-redis-go/internal/server"
	"github.com/Breyes05/mini-redis-go/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "address to listen on")
	aofPath := flag.String("aof", "appendonly.aof", "append-only file path (empty disables persistence)")
	fsync := flag.String("fsync", "everysec", "fsync policy for the AOF: 'always' or 'everysec'")
	replicaOf := flag.String("replicaof", "", "leader address (host:port) to replicate from; empty runs standalone")
	maxMemory := flag.Int64("maxmemory", 0, "approximate memory budget in bytes; evicts least-recently-used keys once exceeded (<= 0 disables the limit)")
	flag.Parse()

	st := store.New()
	defer st.Close()
	st.SetMaxMemory(*maxMemory)

	srv := server.New(*addr, st)

	if *replicaOf != "" {
		// A follower's state comes from its leader's full sync, not its own
		// AOF, so skip local replay — but still open the file (if
		// configured) so this node keeps its own durable copy of what it
		// learns, and mark it read-only so ordinary clients can't write
		// against it directly.
		srv.SetReadOnly(true)
		if *aofPath != "" {
			aof, err := openAOF(*aofPath, *fsync)
			if err != nil {
				log.Fatal(err)
			}
			defer aof.Close()
			srv.SetAOF(aof)
		}
		var offset int64
		srv.SetReplicaOffset(&offset)
		go replication.RunFollower(*replicaOf, srv.Apply, &offset)
	} else if *aofPath != "" {
		log.Printf("replaying %s", *aofPath)
		if err := persistence.Replay(*aofPath, srv.Apply); err != nil {
			log.Fatalf("replay aof: %v", err)
		}
		aof, err := openAOF(*aofPath, *fsync)
		if err != nil {
			log.Fatal(err)
		}
		defer aof.Close()
		srv.SetAOF(aof)
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func openAOF(path, fsync string) (*persistence.AOF, error) {
	policy := persistence.FsyncEverySec
	if fsync == "always" {
		policy = persistence.FsyncAlways
	}
	return persistence.Open(path, policy)
}

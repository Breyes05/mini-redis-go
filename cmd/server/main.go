// Command server starts the mini-redis-go TCP server.
package main

import (
	"flag"
	"log"

	"github.com/Breyes05/mini-redis-go/internal/server"
	"github.com/Breyes05/mini-redis-go/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "address to listen on")
	flag.Parse()

	st := store.New()
	defer st.Close()

	srv := server.New(*addr, st)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

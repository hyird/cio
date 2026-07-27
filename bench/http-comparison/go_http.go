// Minimal HTTP/1.1 server in Go — goroutine per connection.
//
// Raw net.Listener rather than net/http, to match the C++ servers: net/http
// does routing, header parsing and Date formatting that neither of the others
// does, and that difference would swamp the runtime difference this benchmark
// is about.
//
//	./go_http <port> <gomaxprocs>
package main

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
)

var response = []byte("HTTP/1.1 200 OK\r\n" +
	"Server: bench\r\n" +
	"Content-Type: text/plain\r\n" +
	"Content-Length: 13\r\n" +
	"\r\n" +
	"Hello, World!")

var term = [4]byte{'\r', '\n', '\r', '\n'}

// Same framing as the C++ servers: count complete request headers, carrying a
// partial terminator match across reads.
type splitter struct{ matched int }

func (s *splitter) feed(b []byte) int {
	complete := 0
	for _, c := range b {
		if c == term[s.matched] {
			s.matched++
			if s.matched == 4 {
				s.matched = 0
				complete++
			}
		} else if c == '\r' {
			s.matched = 1
		} else {
			s.matched = 0
		}
	}
	return complete
}

func serve(conn *net.TCPConn) {
	defer conn.Close()
	conn.SetNoDelay(true)
	buffer := make([]byte, 2048)
	var s splitter
	for {
		n, err := conn.Read(buffer)
		if n > 0 {
			for i := 0; i < s.feed(buffer[:n]); i++ {
				if _, werr := conn.Write(response); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func main() {
	port := "9304"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	procs := 8
	if len(os.Args) > 2 {
		if p, err := strconv.Atoi(os.Args[2]); err == nil {
			procs = p
		}
	}
	runtime.GOMAXPROCS(procs)

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("go http server — %d procs on %s\n", procs, listener.Addr())
	os.Stdout.Sync()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go serve(conn.(*net.TCPConn))
	}
}

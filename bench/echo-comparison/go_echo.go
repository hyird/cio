// Go echo server — the goroutine-per-connection baseline.
//
// No tuning beyond GOMAXPROCS and TCP_NODELAY: this is what anyone would write,
// which is the point. Go's runtime is work-stealing M:N over a netpoll, the same
// architecture cio has, so this is the closest reference point for it.
//
//	go run go_echo.go <port> <procs>
package main

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
)

func handle(conn *net.TCPConn) {
	defer conn.Close()
	conn.SetNoDelay(true)

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil || n == 0 {
			return
		}
		if _, err := conn.Write(buf[:n]); err != nil {
			return
		}
	}
}

func main() {
	port := 9100
	procs := 8
	if len(os.Args) > 1 {
		port, _ = strconv.Atoi(os.Args[1])
	}
	if len(os.Args) > 2 {
		procs, _ = strconv.Atoi(os.Args[2])
	}
	runtime.GOMAXPROCS(procs)

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen failed:", err)
		os.Exit(1)
	}
	fmt.Printf("go echo server on 0.0.0.0:%d — GOMAXPROCS=%d\n", port, procs)
	os.Stdout.Sync()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go handle(conn.(*net.TCPConn))
	}
}

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"bufio"
	"github.com/ngaut/log"
	"github.com/pingcap/mpdriver/server"
)

var (
	logLevel   = flag.String("L", "debug", "log level: info, debug, warn, error, fatal")
	port       = flag.String("P", "4000", "mp server port")
	addr       = flag.String("A", "127.0.0.1:3306", "mysql address")
	pass       = flag.String("p", "", "mysql password")
	recordPath = flag.String("R", "cmd_record.json", "command record path")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	cfg := &server.Config{
		Addr:     fmt.Sprintf(":%s", *port),
		LogLevel: *logLevel,
	}
	log.SetLevelByString(cfg.LogLevel)
	driver := &server.MysqlDriver{Addr: *addr, Pass: *pass}
	svr, err := server.NewServer(cfg, driver)
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.OpenFile(*recordPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	bufWriter := bufio.NewWriter(file)
	server.CommandRecordWriter = bufWriter

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		svr.Close()
		bufWriter.Flush()
		file.Close()
		os.Exit(0)
	}()

	log.Error(svr.Run())
}

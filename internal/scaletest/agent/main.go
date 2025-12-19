package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path"
	"strconv"

	"github.com/open-telemetry/opamp-go/internal/scaletest/agent/internal"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	//shutdownFunc := initMeter()
	//defer shutdownFunc()

	curDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	var agentCount int
	flag.IntVar(&agentCount, "n", 1, "agent count")

	var agentType string
	flag.StringVar(&agentType, "t", "io.opentelemetry.collector", "Agent Type String")

	var agentVersion string
	flag.StringVar(&agentVersion, "v", "1.0.0", "Agent Version String")

	flag.Parse()

	agents := []*internal.Agent{}

	log.Printf("Creating %d agents...", agentCount)
	for i := 0; i < agentCount; i++ {
		dataDir := path.Join(curDir, "datadir", agentType, strconv.Itoa(i))
		os.MkdirAll(dataDir, 0o755)

		var logger *log.Logger
		if agentCount == 1 {
			logger = log.Default()
		} else {
			logFile, err := os.Create(path.Join(dataDir, "agent.log"))
			if err != nil {
				panic(err)
			}
			logger = log.New(logFile, "", log.Ldate|log.Ltime|log.Lmicroseconds)
		}
		agent := internal.NewAgent(dataDir, logger, agentType, agentVersion)
		agents = append(agents, agent)
	}
	log.Printf("Created %d agents.", agentCount)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	for _, agent := range agents {
		agent.Shutdown()
	}
}

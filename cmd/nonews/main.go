/*
   nonews - NNTP indexer
   Copyright (C) 2014  Casey Marshall

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, version 3.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cmars/nonews"
	"github.com/juju/errors"
	"github.com/juju/loggo"
)

var configFile *string = flag.String("config", "", "Config file")
var logLevelName *string = flag.String("loglevel", "WARNING", "Log level")
var nconn *int = flag.Int("nconn", 3, "Number of news server connections")

var logger loggo.Logger = loggo.GetLogger("nonews.main")

func die(err error) {
	logger.Errorf("%s", errors.ErrorStack(err))
	os.Exit(1)
}

func main() {
	flag.Parse()
	if *configFile == "" {
		die(fmt.Errorf("-config flag required"))
	}

	logLevel, ok := loggo.ParseLevel(*logLevelName)
	if !ok {
		die(fmt.Errorf("-logLevel invalid (use TRACE, DEBUG, INFO, WARN, ERROR, CRITICAL)"))
	}
	loggo.Logger{}.SetLogLevel(logLevel)

	config, err := nonews.LoadConfig(*configFile)
	if err != nil {
		die(err)
	}

	client := nonews.NewClient(config)
	client.Start(*nconn)

	for _, group := range config.IndexGroups() {
		indexer, err := nonews.NewIndexer(group, config)
		if err != nil {
			die(err)
		}
		indexer.Start(client)
	}
	fmt.Println("started")
	<-chan struct{}(nil)
}

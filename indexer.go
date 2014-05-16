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

package nonews

import (
	"fmt"
	"time"

	"github.com/cmars/nntp"
	"github.com/juju/errors"
	"github.com/juju/loggo"
	"labix.org/v2/mgo"
)

const DBNAME = "nonews"

var logger loggo.Logger = loggo.GetLogger("nonews.indexer")

type Indexer struct {
	Config *Config
	Group  string

	conn    *nntp.Conn
	session *mgo.Session
}

func NewIndexer(config *Config, group string) (*Indexer, error) {
	var err error
	indexer := &Indexer{Config: config, Group: group}

	indexer.conn, err = dialNntp(config)
	if err != nil {
		return nil, err
	}

	indexer.session, err = dialMongo(config)
	if err != nil {
		return nil, err
	}

	err = indexer.ensureIndexes()
	if err != nil {
		return nil, err
	}
	return indexer, nil
}

func dialNntp(config *Config) (*nntp.Conn, error) {
	var conn *nntp.Conn
	var err error

	if config.TLS() {
		conn, err = nntp.DialTLS("tcp", config.Addr(), nil)
	} else {
		conn, err = nntp.Dial("tcp", config.Addr())
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	if config.Username() != "" {
		err = conn.Authenticate(config.Username(), config.Password())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return conn, nil
}

func dialMongo(config *Config) (*mgo.Session, error) {
	session, err := mgo.Dial(config.MongoURL())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return session, nil
}

func (idx *Indexer) ensureIndexes() error {
	// TODO: set up indexes on newsgroup collections
	return nil
}

func (idx *Indexer) Index() {
	var from, to int
	var loaded int
	for {
		logger.Infof("scanning %s", idx.Group)
		_, low, high, err := idx.conn.Group(idx.Group)
		if err != nil {
			err = errors.Trace(err)
			goto DELAY
		}
		logger.Debugf("group %s: low=%d, high=%d", idx.Group, low, high)
		if high-low <= 0 {
			logger.Warningf("empty group: %s", idx.Group)
			goto DELAY
		}
		if from == 0 {
			from = high - 100
			to = high
		} else {
			from = to
			to = high
		}
		if from-to == 0 {
			logger.Debugf("no new news")
			goto DELAY
		}

		loaded = 0
		for i := from; i <= to; i++ {
			article, err := idx.conn.Head(fmt.Sprintf("%d", i))
			if err != nil {
				err = errors.Trace(err)
				goto DELAY
			}
			err = idx.session.DB(DBNAME).C(idx.Group).Insert(article)
			if err != nil {
				err = errors.Trace(err)
				goto DELAY
			}
			loaded++
		}

	DELAY:
		if loaded > 0 {
			logger.Infof("group %s: loaded %d new headers", idx.Group, loaded)
		}
		if err != nil {
			logger.Errorf("%s", errors.ErrorStack(err))
		}
		delay := time.Duration(idx.Config.GroupDelay(idx.Group)) * time.Second
		logger.Tracef("sleeping %v", delay)
		time.Sleep(delay)
	}
}

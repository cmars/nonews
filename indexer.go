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
	"sync"
	"time"

	"github.com/cmars/nntp"
	"github.com/juju/errors"
	"github.com/juju/loggo"
	"labix.org/v2/mgo"
)

const DBNAME = "nonews"

var logger loggo.Logger = loggo.GetLogger("nonews")

type Indexer struct {
	Config *Config
	Group  string

	mutex   *sync.Mutex
	conn    *nntp.Conn
	session *mgo.Session
}

func NewIndexer(config *Config, group string) (*Indexer, error) {
	var err error
	indexer := &Indexer{Config: config, Group: group, mutex: &sync.Mutex{}}

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
	err := idx.session.DB(DBNAME).C(idx.Group).EnsureIndex(mgo.Index{
		Key:      []string{"header.Message-Id"},
		Unique:   true,
		DropDups: true,
	})
	return err
}

func (idx *Indexer) Start() {
	groupChan := idx.discoverArticles()
	headerChan := idx.fetchArticles(groupChan)
	idx.loadArticles(headerChan)
}

func (idx *Indexer) discoverArticles() chan *nntp.Group {
	groupChan := make(chan *nntp.Group)

	go func() {
		var err error
		var group *nntp.Group
		logger := loggo.GetLogger(idx.Group + ".discover")
		for {
			idx.mutex.Lock()
			group, err = idx.conn.Group(idx.Group)
			idx.mutex.Unlock()
			if err != nil {
				goto DELAY
			}
			logger.Tracef("%v", group)
			groupChan <- group
		DELAY:
			if err != nil {
				logger.Errorf(errors.ErrorStack(err))
			}
			idx.delay()
		}
	}()
	return groupChan
}

func (idx *Indexer) delay() {
	delay := time.Duration(idx.Config.GroupDelay(idx.Group)) * time.Second
	time.Sleep(delay)
}

func (idx *Indexer) fetchArticles(groupChan chan *nntp.Group) chan *nntp.Article {
	articleChan := make(chan *nntp.Article)

	var last int
	logger := loggo.GetLogger(idx.Group + ".headers")

	go func() {
		for group := range groupChan {
			func() {
				idx.mutex.Lock()
				defer idx.mutex.Unlock()
				defer logger.Debugf("done, last=%d", last)

				var err error

				if last == 0 {
					last = group.High - 100
				}
				if group.High <= last {
					logger.Debugf("no new articles")
					return
				}

				logger.Debugf("fetching headers for %d-%d", last, group.High)

				for i := last; i <= group.High; i++ {
					article, headErr := idx.conn.Head(fmt.Sprintf("%d", i))
					if headErr != nil {
						if err == nil {
							err = headErr
						}
						err = errors.Annotatef(headErr, "HEAD %d", i)
						continue
					}
					articleChan <- article
					last = i
				}
				if err != nil {
					logger.Errorf(errors.ErrorStack(err))
				}
			}()
		}
	}()

	return articleChan
}

func (idx *Indexer) loadArticles(articles chan *nntp.Article) {
	logger := loggo.GetLogger(idx.Group + ".loader")
	go func() {
		i := 0
		for article := range articles {
			err := idx.session.DB(DBNAME).C(idx.Group).Insert(article)
			if mgo.IsDup(err) {
				logger.Tracef("already have %v", article)
				continue
			} else if err != nil {
				err = errors.Annotatef(err, "%v", article)
				logger.Errorf("insert failed: %v", errors.ErrorStack(err))
			}

			i++
			if i%100 == 0 {
				logger.Debugf("loaded %d headers", i)
			}
		}
	}()
}

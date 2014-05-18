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

var logger loggo.Logger = loggo.GetLogger("nonews")

type Indexer struct {
	Config *Config
	Group  string

	session *mgo.Session

	groupReq  chan *groupReq
	groupResp chan *groupResp
	headsReq  chan *headsReq
	headsResp chan *headsResp
}

func NewIndexer(config *Config, group string) (*Indexer, error) {
	var err error
	indexer := &Indexer{
		Config: config,
		Group:  group,

		groupReq:  make(chan *groupReq),
		groupResp: make(chan *groupResp),
		headsReq:  make(chan *headsReq),
		headsResp: make(chan *headsResp),
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
	go idx.nntpClient()
	groupChan := idx.discoverArticles()
	headerChan := idx.fetchArticles(groupChan)
	idx.loadArticles(headerChan)
}

type groupReq struct {
}

type groupResp struct {
	Group *nntp.Group
	Error error
}

type headsReq struct {
	Start, End int
}

type headsResp struct {
	Articles []*nntp.Article
	Last     int
	Error    error
}

func (idx *Indexer) group(name string) (*nntp.Group, error) {
	go func() {
		idx.groupReq <- &groupReq{}
	}()
	resp := <-idx.groupResp
	return resp.Group, resp.Error
}

func (idx *Indexer) articles(start, end int) ([]*nntp.Article, int, error) {
	go func() {
		idx.headsReq <- &headsReq{Start: start, End: end}
	}()
	resp := <-idx.headsResp
	return resp.Articles, resp.Last, resp.Error
}

func (idx *Indexer) nntpClient() {
	var conn *nntp.Conn
	logger := loggo.GetLogger(idx.Group + ".client")
	for {
		var err error

		if conn == nil {
			conn, err = dialNntp(idx.Config)
			if err != nil {
				logger.Errorf("connect failed: %v", err)
				time.Sleep(3 * time.Second)
				continue
			}
		}

		select {
		case _ = <-idx.groupReq:
			var group *nntp.Group
			group, err = conn.Group(idx.Group)
			idx.groupResp <- &groupResp{Group: group, Error: err}

		case headsReq := <-idx.headsReq:
			headsResp := &headsResp{}
			_, err = conn.Group(idx.Group)
			if err != nil {
				headsResp.Error = err
			}

			for i := headsReq.Start; err == nil && i <= headsReq.End; i++ {
				article, headErr := conn.Head(fmt.Sprintf("%d", i))
				if errors.Check(headErr, nntp.IsProtocol) {
					err = headErr
					headsResp.Error = errors.Trace(headErr)
				} else if nntp.ErrorCode(headErr) == 423 {
					logger.Tracef("%v: %d", headErr, i)
					continue
				} else if headErr != nil {
					headsResp.Error = errors.Annotatef(headsResp.Error, "HEAD %d: %v", i, headErr)
				} else {
					headsResp.Articles = append(headsResp.Articles, article)
					headsResp.Last = i
				}
			}
			idx.headsResp <- headsResp
		}

		if err != nil {
			logger.Errorf("%s", errors.ErrorStack(err))
			conn.Quit()
			conn = nil
		}
	}
}

func (idx *Indexer) discoverArticles() chan *nntp.Group {
	groupChan := make(chan *nntp.Group)

	go func() {
		var err error
		var group *nntp.Group
		logger := loggo.GetLogger(idx.Group + ".discover")
		for {
			group, err = idx.group(idx.Group)
			if err != nil {
				goto DELAY
			}
			logger.Debugf("%v", group)
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

	var start int
	logger := loggo.GetLogger(idx.Group + ".headers")

	go func() {
		for group := range groupChan {
			var err error

			if start == 0 {
				start = group.High - 100
			}
			if group.High <= start {
				logger.Debugf("no new articles")
				return
			}

			logger.Debugf("fetching headers for %d-%d", start, group.High)

			articles, last, err := idx.articles(start, group.High)
			if err != nil {
				logger.Errorf("%v", err)
				logger.Tracef(errors.ErrorStack(err))
				continue
			}

			for _, article := range articles {
				articleChan <- article
			}

			logger.Debugf("done, last=%d", last)

			if last > 0 {
				start = last + 1
			}
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
				logger.Debugf("loaded up to %d headers", i)
			}
		}
	}()
}

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
	"time"

	"github.com/cmars/nntp"
	"github.com/juju/errors"
	"github.com/juju/loggo"
	"labix.org/v2/mgo"
)

const (
	DBNAME        = "nonews"
	HeadChunkSize = 25
)

type Indexer struct {
	Config *Config
	Group  string

	session *mgo.Session
}

func NewIndexer(group string, config *Config) (*Indexer, error) {
	var err error
	indexer := &Indexer{
		Config: config,
		Group:  group,
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

func (idx *Indexer) Start(client *Client) {
	groupChan := idx.discoverArticles(client)
	headerChan := idx.fetchArticles(client, groupChan)
	idx.loadArticles(headerChan)
}

func (idx *Indexer) discoverArticles(client *Client) chan *nntp.Group {
	groupChan := make(chan *nntp.Group)

	go func() {
		var err error
		var group *nntp.Group
		logger := loggo.GetLogger(idx.Group + ".discover")
		for {
			group, err = client.Group(idx.Group)
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

func (idx *Indexer) fetchArticles(client *Client, groupChan chan *nntp.Group) chan *nntp.Article {
	articleChan := make(chan *nntp.Article)

	var start int
	logger := loggo.GetLogger(idx.Group + ".headers")

	go func() {
		for group := range groupChan {

			if start == 0 {
				start = group.High - 100
			}
			if group.High <= start {
				logger.Debugf("no new articles")
				return
			}

			for i := start; i < group.High; i += HeadChunkSize {
				from := i
				var to int
				if i+HeadChunkSize >= group.High {
					to = group.High - 1
				} else {
					to = i + HeadChunkSize - 1
				}
				go func() {
					logger.Debugf("fetching headers for %d-%d", from, to)
					resp := client.Articles(idx.Group, from, to)
					for articleHead := range resp {
						if articleHead.Error != nil {
							// TODO: retry failed articles
						} else {
							articleChan <- articleHead.Article
						}
					}
				}()
			}

			start = group.High
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

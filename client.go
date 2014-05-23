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
)

type Client struct {
	config    *Config
	groupReq  chan *groupReq
	groupResp chan *groupResp
	headsReq  chan *headsReq
	headsResp chan *headsResp
}

func NewClient(config *Config) *Client {
	return &Client{
		config:    config,
		groupReq:  make(chan *groupReq),
		groupResp: make(chan *groupResp),
		headsReq:  make(chan *headsReq),
		headsResp: make(chan *headsResp),
	}
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

func (c *Client) Start(n int) {
	for i := 0; i < n; i++ {
		go c.nntpClient()
	}
}

type groupReq struct {
	Name string
}

type groupResp struct {
	Group *nntp.Group
	Error error
}

type headsReq struct {
	Group      string
	Start, End int
}

type Article struct {
	*nntp.Article
	Number   int
	Error    error
	ErrCount int
}

type headsResp struct {
	Heads chan *Article
	Error error
}

func (c *Client) Group(name string) (*nntp.Group, error) {
	go func() {
		c.groupReq <- &groupReq{Name: name}
	}()
	resp := <-c.groupResp
	return resp.Group, resp.Error
}

func (c *Client) Articles(group string, start, end int) chan *Article {
	go func() {
		c.headsReq <- &headsReq{Group: group, Start: start, End: end}
	}()
	resp := <-c.headsResp
	return resp.Heads
}

func (c *Client) nntpClient() {
	var conn *nntp.Conn
	logger := loggo.GetLogger("nonews.client")
	for {
		var err error

		if conn == nil {
			conn, err = dialNntp(c.config)
			if err != nil {
				logger.Errorf("connect failed: %v", err)
				time.Sleep(3 * time.Second)
				continue
			}
		}

		select {
		case req := <-c.groupReq:
			var group *nntp.Group
			group, err = conn.Group(req.Name)
			c.groupResp <- &groupResp{Group: group, Error: err}

		case req := <-c.headsReq:
			headsResp := &headsResp{Heads: make(chan *Article)}
			_, err = conn.Group(req.Group)
			if err != nil {
				headsResp.Error = err
			}

			c.headsResp <- headsResp

			for i := req.Start; i <= req.End; i++ {
				article, headErr := conn.Head(fmt.Sprintf("%d", i))
				if errors.Check(headErr, nntp.IsProtocol) {
					err = headErr
					headsResp.Heads <- &Article{
						Number: i,
						Error:  errors.Trace(headErr),
					}
					headsResp.Error = errors.Trace(headErr)
				} else if nntp.ErrorCode(headErr) == 423 {
					logger.Tracef("%v: %d", headErr, i)
					continue
				} else if headErr != nil {
					headsResp.Heads <- &Article{
						Number: i,
						Error:  errors.Trace(headErr),
					}
				} else {
					headsResp.Heads <- &Article{Article: article, Number: i}
				}
			}
			close(headsResp.Heads)
		}

		if err != nil {
			logger.Errorf("%s", errors.ErrorStack(err))
			conn.Quit()
			conn = nil
		}
	}
}

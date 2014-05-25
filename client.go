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
	"strconv"
	"strings"
	"time"

	"github.com/cmars/nntp"
	"github.com/juju/errors"
	"github.com/juju/loggo"
)

var logger = loggo.GetLogger("nonews")

type Client struct {
	config   *Config
	groupReq chan *groupReq
	headsReq chan *headsReq
}

func NewClient(config *Config) *Client {
	return &Client{
		config:   config,
		groupReq: make(chan *groupReq),
		headsReq: make(chan *headsReq),
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
	Resp chan *Group
}

type headsReq struct {
	Group      string
	Start, End int
	Resp       chan *Article
}

type PartType string

const (
	NfoPart  = PartType("nfo")
	SfvPart  = PartType("sfv")
	RarPart  = PartType("rar")
	Par2Part = PartType("par2")
)

type Part struct {
	Collection string
	Num        int
	Total      int
	Type       PartType
}

type Xref struct {
	Server string `bson:",omitempty"`
	Group  string
	Number int
}

type Group struct {
	*nntp.Group

	Errors []error
}

type Article struct {
	Header map[string][]string

	MessageId string
	Subject   string
	Xref      []Xref
	Timestamp int64

	Part Part

	Errors []error `bson:",omitempty"`
}

var ErrMalformedXref = fmt.Errorf("malformed xref")

func ParseXref(xref string) ([]Xref, error) {
	var result []Xref
	wsplit := strings.SplitN(xref, " ", 2)
	if len(wsplit) < 2 {
		return nil, ErrMalformedXref
	}
	server := wsplit[0]
	wsplit = strings.Split(wsplit[1], " ")
	for _, part := range wsplit {
		colsplit := strings.Split(part, ":")
		if len(colsplit) < 2 {
			return nil, ErrMalformedXref
		}
		num, err := strconv.Atoi(colsplit[1])
		if err != nil {
			return nil, err
		}
		result = append(result, Xref{
			Server: server,
			Group:  colsplit[0],
			Number: num,
		})
	}
	return result, nil
}

var postingDateFormats = []string{
	"Mon, 2 Jan 2006 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04:05 MST",
}

func ParsePostingDate(s string) (time.Time, error) {
	var err error
	for _, format := range postingDateFormats {
		t, err := time.Parse(format, s)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, err
}

func NewArticle(a *nntp.Article) *Article {
	article := &Article{Header: a.Header}
	if messageId, ok := a.Header["Message-Id"]; ok && len(messageId) > 0 {
		article.MessageId = a.Header["Message-Id"][0]
	} else {
		article.Errors = append(article.Errors, fmt.Errorf("missing Message-Id"))
	}

	if subject, ok := a.Header["Subject"]; ok && len(subject) > 0 {
		article.Subject = a.Header["Subject"][0]
	} else {
		article.Errors = append(article.Errors, fmt.Errorf("missing Subject"))
	}

	if xref, ok := a.Header["Xref"]; ok && len(xref) > 0 {
		var err error
		if article.Xref, err = ParseXref(xref[0]); err != nil {
			article.Errors = append(article.Errors, err)
		}
	} else {
		article.Errors = append(article.Errors, fmt.Errorf("missing Xref"))
	}

	if postingDate, ok := a.Header["Date"]; ok && len(postingDate) > 0 {
		if t, err := ParsePostingDate(postingDate[0]); err != nil {
			article.Errors = append(article.Errors, err)
		} else {
			article.Timestamp = t.Unix()
		}
	} else {
		article.Errors = append(article.Errors, fmt.Errorf("missing Date"))
	}

	article.stripHeaders()
	return article
}

var stripHeaders = []string{
	"Message-Id",
	"Subject",
	"Xref",
	"Date",
	"Organization",
	"Path",
}

func (a *Article) stripHeaders() {
	for _, header := range stripHeaders {
		delete(a.Header, header)
	}
}

func (c *Client) Group(name string) chan *Group {
	resp := make(chan *Group)
	go func() { c.groupReq <- &groupReq{Name: name, Resp: resp} }()
	return resp
}

func (c *Client) Articles(group string, start, end int) chan *Article {
	resp := make(chan *Article)
	go func() {
		c.headsReq <- &headsReq{Group: group, Start: start, End: end, Resp: resp}
	}()
	return resp
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
			resp := &Group{}
			var group *nntp.Group
			group, err = conn.Group(req.Name)
			if err != nil {
				resp.Errors = append(resp.Errors, err)
			} else {
				resp.Group = group
			}
			req.Resp <- resp
			close(req.Resp)

		case req := <-c.headsReq:
			_, err = conn.Group(req.Group)
			for i := req.Start; i <= req.End; i++ {
				article, headErr := conn.Head(fmt.Sprintf("%d", i))
				if errors.Check(headErr, nntp.IsProtocol) {
					err = headErr
					req.Resp <- &Article{
						Xref: []Xref{
							Xref{
								Group:  req.Group,
								Number: i,
							},
						},
						Errors: []error{errors.Trace(headErr)},
					}
				} else if nntp.ErrorCode(headErr) == 423 {
					logger.Tracef("%v: %d", headErr, i)
					continue
				} else if headErr != nil {
					req.Resp <- &Article{
						Xref: []Xref{
							Xref{
								Group:  req.Group,
								Number: i,
							},
						},
						Errors: []error{errors.Trace(headErr)},
					}
				} else {
					req.Resp <- NewArticle(article)
				}
			}
			close(req.Resp)
		}

		if err != nil {
			logger.Errorf("%s", errors.ErrorStack(err))
			conn.Quit()
			conn = nil
		}
	}
}

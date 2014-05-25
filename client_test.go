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

package nonews_test

import (
	"testing"

	gc "github.com/cmars/check"
	"github.com/cmars/nntp"

	"github.com/cmars/nonews"
)

func Test(t *testing.T) { gc.TestingT(t) }

type Suite struct{}

var _ = gc.Suite(&Suite{})

func (s *Suite) TestArticles(c *gc.C) {
	testCases := []struct {
		Article *nntp.Article
		Subject string
		Xref    []nonews.Xref
	}{
		{
			Article: &nntp.Article{
				Header: map[string][]string{
					"Message-Id":        []string{"qwerty"},
					"Subject":           []string{"Human Cannonball"},
					"Xref":              []string{"news-server.com alt.music.psych:78901 alt.music:12345"},
					"Nntp-Posting-Date": []string{"Sat, 24 May 2014 03:40:29 UTC"},
				},
			},
			Subject: "Human Cannonball",
			Xref: []nonews.Xref{
				nonews.Xref{
					Server: "news-server.com",
					Group:  "alt.music.psych",
					Number: 78901,
				},
				nonews.Xref{
					Server: "news-server.com",
					Group:  "alt.music",
					Number: 12345,
				},
			},
		},
		{
			Article: &nntp.Article{
				Header: map[string][]string{
					"Message-Id":        []string{"asdfgh"},
					"Subject":           []string{"Burn Hollywood Burn"},
					"Xref":              []string{"news-server.com alt.music.rap:90123 alt.music:23456"},
					"Nntp-Posting-Date": []string{"24 May 2014 05:30:31 GMT"},
				},
			},
			Subject: "Burn Hollywood Burn",
			Xref: []nonews.Xref{
				nonews.Xref{
					Server: "news-server.com",
					Group:  "alt.music.rap",
					Number: 90123,
				},
				nonews.Xref{
					Server: "news-server.com",
					Group:  "alt.music",
					Number: 23456,
				},
			},
		},
		{
			Article: &nntp.Article{
				Header: map[string][]string{
					"Message-Id":        []string{"zxcvbn"},
					"Subject":           []string{"Fok Julle Naaiers"},
					"Xref":              []string{"news-server.com alt.music:56789"},
					"Nntp-Posting-Date": []string{"Sun, 25 May 2014 05:05:33 +0200"},
				},
			},
			Subject: "Fok Julle Naaiers",
			Xref: []nonews.Xref{
				nonews.Xref{
					Server: "news-server.com",
					Group:  "alt.music",
					Number: 56789,
				},
			},
		},
	}
	for _, testCase := range testCases {
		article := nonews.NewArticle(testCase.Article)
		c.Assert(article.Subject, gc.Equals, testCase.Subject)
		c.Assert(article.Xref, gc.DeepEquals, testCase.Xref)
	}
}

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

	"github.com/juju/errors"
	"github.com/pelletier/go-toml"
)

type Config struct {
	*toml.TomlTree
}

func (c *Config) Valid() error {
	if c.Addr() == "" {
		return errors.Trace(fmt.Errorf("missing 'nonews.addr'"))
	}
	return nil
}

func (c *Config) getString(key string) string {
	v := c.Get(key)
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func (c *Config) getBool(key string) bool {
	v := c.Get(key)
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func (c *Config) Addr() string {
	return c.getString("nonews.addr")
}

func (c *Config) Username() string {
	return c.getString("nonews.username")
}

func (c *Config) Password() string {
	return c.getString("nonews.password")
}

func (c *Config) TLS() bool {
	return c.getBool("nonews.tls")
}

func (c *Config) MongoURL() string {
	return c.getString("nonews.mongo.url")
}

func (c *Config) IndexGroups() []string {
	v := c.Get("nonews.index.groups")
	if li, ok := v.([]interface{}); ok {
		var ls []string
		for _, i := range li {
			if s, ok := i.(string); ok {
				ls = append(ls, s)
			}
		}
		return ls
	}
	return nil
}

const defaultGroupDelay = 300

func (c *Config) GroupDelay(group string) int {
	key := fmt.Sprintf("%s.delay", group)
	v := c.Get(key)
	logger.Tracef("key=%s v=%v", key, v)
	if i, ok := v.(int64); ok {
		return int(i)
	}
	return defaultGroupDelay
}

func LoadConfig(path string) (*Config, error) {
	tree, err := toml.LoadFile(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cfg := &Config{tree}
	return cfg, cfg.Valid()
}

/*
 * Nanocloud Community, a comprehensive platform to turn any application
 * into a cloud solution.
 *
 * Copyright (C) 2015 Nanocloud Software
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/dullgiulio/pingo"
)

type HistoryConfig struct {
	ConnectionString string
	DatabaseName     string
}

type History struct {
	g_HistoryConfig HistoryConfig
	g_HistoryDb     *bolt.DB
}

type HistoryParam struct {
	UserId       string
	ConnectionId string
	StartDate    string
	EndDate      string
}

type HistoryInfo struct {
	UserId       string
	ConnectionId string
	Stats        []HistoryAtom
}

type HistoryAtom struct {
	StartDate string
	EndDate   string
}

func (p *History) Configure(jsonConfig string, _outMsg *string) error {
	var (
		historyConfig map[string]string
	)

	err := json.Unmarshal([]byte(jsonConfig), &historyConfig)
	if err != nil {
		r := fmt.Sprintf("ERROR: failed to unmarshal History Plugin configuration : %s", err.Error())
		log.Printf(r)
		os.Exit(0)
		*_outMsg = r
		return nil
	}

	p.g_HistoryConfig.ConnectionString = historyConfig["ConnectionString"]
	p.g_HistoryConfig.DatabaseName = historyConfig["DatabaseName"]
	p.g_HistoryDb, err = bolt.Open(p.g_HistoryConfig.ConnectionString, 0777, nil)
	if err != nil {
		return err
	}

	err = p.g_HistoryDb.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte(p.g_HistoryConfig.DatabaseName))
		return nil
	})
	return err
}

func (p *History) GetList(args string, histories *[]HistoryInfo) error {
	var (
		history HistoryInfo
		stats   []HistoryAtom
	)
	fmt.Println("GET HISTORY")

	e := p.g_HistoryDb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(p.g_HistoryConfig.DatabaseName))
		if bucket == nil {
			return errors.New(fmt.Sprintf("Bucket '%s' doesn't exist", p.g_HistoryConfig.DatabaseName))
		}

		cursor := bucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			json.Unmarshal(value, &stats)
			history = HistoryInfo{
				UserId:       string(key),
				ConnectionId: string(key),
				Stats:        stats,
			}
			*histories = append(*histories, history)
		}

		return nil
	})

	if e != nil {
		return e
	}

	return nil
}

func (p *History) Add(jsonParams string, _outMsg *string) error {
	var (
		params HistoryParam
		stats  []HistoryAtom
	)

	e := json.Unmarshal([]byte(jsonParams), &params)
	if e != nil {
		r := fmt.Sprintf("ERROR: failed to unmarshal GetList params : %s", e.Error())
		log.Printf(r)
		os.Exit(0)
		*_outMsg = r
		return nil
	}

	if _, err := time.Parse("Mon Jan 2 15:04:05 MST 2006", params.StartDate); err != nil {
		return errors.New(fmt.Sprintf("Can't parse StartDate : «%s»", params.StartDate))
	}
	if _, err := time.Parse("Mon Jan 2 15:04:05 MST 2006", params.EndDate); err != nil {
		return errors.New(fmt.Sprintf("Can't parse EndDate : «%s»", params.StartDate))
	}

	e = p.g_HistoryDb.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(p.g_HistoryConfig.DatabaseName))
		if bucket == nil {
			return errors.New(fmt.Sprintf("Bucket '%s' doesn't exist", p.g_HistoryConfig.DatabaseName))
		}

		statsJson := bucket.Get([]byte(params.ConnectionId))
		json.Unmarshal(statsJson, &stats)

		stats = append(stats, HistoryAtom{
			StartDate: params.StartDate,
			EndDate:   params.EndDate,
		})
		jsonHistory, e := json.Marshal(stats)
		bucket.Put([]byte(params.ConnectionId), jsonHistory)

		return e
	})

	*_outMsg = "true"
	return nil
}

func main() {

	plugin := &History{}

	pingo.Register(plugin)

	pingo.Run()
}

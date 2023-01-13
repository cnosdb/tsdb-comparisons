package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/valyala/fasthttp"
)

type dbCreator struct {
	daemonURL string
}

func (d *dbCreator) Init() {
	d.daemonURL = daemonURLs[0] // pick first one since it always exists
}

func (d *dbCreator) DBExists(dbName string) bool {
	dbs, err := d.listDatabases()
	if err != nil {
		log.Fatal(err)
	}

	for _, db := range dbs {
		if db == loader.DatabaseName() {
			return true
		}
	}
	return false
}

func (d *dbCreator) listDatabases() ([]string, error) {
	u := fmt.Sprintf("%s/api/v1/sql", d.daemonURL)
	sql := []byte("SHOW DATABASES")
	req, err := http.NewRequest("POST", u, bytes.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.Header.Add(fasthttp.HeaderAuthorization, basicAuth("root", ""))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listDatabases db error: %s", err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Do ad-hoc parsing to find existing database names:
	// [{"Database":"public"}]%
	type listingType struct {
		Database string
	}
	var listingValues []listingType
	err = json.Unmarshal(body, &listingValues)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, db := range listingValues {
		ret = append(ret, db.Database)
	}
	return ret, nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	u := fmt.Sprintf("%s/api/v1/sql", d.daemonURL)
	sql := []byte("DROP DATABASE IF EXISTS " + dbName)
	req, err := http.NewRequest("POST", u, bytes.NewReader(sql))
	if err != nil {
		return err
	}
	req.Header.Add(fasthttp.HeaderAuthorization, basicAuth("root", ""))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("drop db error: %s", err.Error())
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		respMsg, err := io.ReadAll(resp.Body)
		if err == nil {
			return fmt.Errorf("drop db returned non-200 code: %d: %s", resp.StatusCode, respMsg)
		} else {
			return fmt.Errorf("drop db returned non-200 code: %d", resp.StatusCode)
		}
	}

	time.Sleep(time.Second)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	u := fmt.Sprintf("%s/api/v1/sql", d.daemonURL)
	sql := []byte("CREATE DATABASE " + dbName)
	req, err := http.NewRequest("POST", u, bytes.NewReader(sql))
	if err != nil {
		return err
	}
	req.Header.Add(fasthttp.HeaderAuthorization, basicAuth("root", ""))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("create db error: %s", err.Error())
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		respMsg, err := io.ReadAll(resp.Body)
		if err == nil {
			return fmt.Errorf("drop db returned non-200 code: %d: %s", resp.StatusCode, respMsg)
		} else {
			return fmt.Errorf("create db returned non-200 code: %d", resp.StatusCode)
		}
	}

	time.Sleep(time.Second)
	return nil
}

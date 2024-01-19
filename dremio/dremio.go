//      Copyright 2023 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package dremio is the entry point for the REST API
package dremio

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// ProtocolArgs is used to setup the connection
type ProtocolArgs struct {
	Timeout        time.Duration
	User           string
	Password       string
	URL            string
	CloudProjectID string
}

// HTTPProtocolEngine uses HTTP calls against the Dremio REST API
type HTTPProtocolEngine struct {
	token                string
	client               http.Client
	queryTimeoutDuration time.Duration
	queryURL             string
	queryJobsURL         string
	sleepTimeSeconds     float64
	totalRetries         int
}

// SQL submits the query against the api and returns the results. It will repeatedly query the job status to see when the result is done
func (h *HTTPProtocolEngine) SQL(query string) ([]map[string]interface{}, error) {
	data := map[string]string{
		"sql": query,
	}
	jsonBody, err := json.Marshal(data)
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("unable to create sql json: %w", err)
	}
	url := h.queryURL
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("unable to create request %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", h.token)

	res, err := h.client.Do(req)
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("failed sending login request: %w", err)
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("could not read response body: %w", err)
	}
	var resultMap map[string]interface{}
	err = json.Unmarshal(resBody, &resultMap)
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("could not read %v json %w", string(resBody), err)
	}

	v, ok := resultMap["id"]
	if ok {
		token := fmt.Sprintf("%v", v)
		if token == "" {
			return []map[string]interface{}{}, errors.New("blank id cannot proceed")
		}
		status, err := h.checkQueryStatus(fmt.Sprintf("%v", v))
		if err != nil {
			return []map[string]interface{}{}, err
		}
		if status == "COMPLETED" {
			rows, err := h.getRows(fmt.Sprintf("%v", v))
			if err != nil {
				return []map[string]interface{}{}, fmt.Errorf("unable to get rows '%v'", err)
			}
			return rows, nil
		}
		return []map[string]interface{}{}, fmt.Errorf("query status not complete but '%v'", status)
	}
	return []map[string]interface{}{}, fmt.Errorf("no job id in response %#v for url %v so failing the query", resultMap, url)
}

func (h *HTTPProtocolEngine) getRows(id string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	counter := 0
	for {
		rows, err := h.getPage(id, counter)
		if err != nil {
			return []map[string]interface{}{}, fmt.Errorf("failed on page number %v: %v", counter+1, err)
		}
		if len(rows) == 0 {
			break
		}
		results = append(results, rows...)
		counter += 100
	}
	return results, nil
}

func (h *HTTPProtocolEngine) getPage(id string, offset int) ([]map[string]interface{}, error) {
	// default is 100 rows
	url := fmt.Sprintf("%v/%v/results?offset=%v&limit=100", h.queryJobsURL, id, offset)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("unable to create request %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", h.token)

	res, err := h.client.Do(req)
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("failed sending login request: %w", err)
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("client: could not read response body: %s", err)
	}
	var resultMap map[string]interface{}
	err = json.Unmarshal(resBody, &resultMap)
	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("client: could not read json: %s", err)
	}
	rows, ok := resultMap["rows"]
	if !ok {
		return []map[string]interface{}{}, fmt.Errorf("client: json missing rows key %v", string(resBody))
	}
	rawRowArray := rows.([]interface{})
	var typedResults []map[string]interface{}
	for _, row := range rawRowArray {
		typedResults = append(typedResults, row.(map[string]interface{}))
	}
	return typedResults, nil
}

func (h *HTTPProtocolEngine) checkQueryStatus(id string) (status string, err error) {
	url := fmt.Sprintf("%v/%v", h.queryJobsURL, id)

	var lastState string
	for i := 0; i < h.totalRetries; i++ {
		duration := time.Duration(h.sleepTimeSeconds*1000) * time.Millisecond
		time.Sleep(duration)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return "", fmt.Errorf("unable to create request %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", h.token)

		res, err := h.client.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed sending login request: %w", err)
		}
		resBody, err := io.ReadAll(res.Body)
		if err != nil {
			return "", fmt.Errorf("client: could not read response body: %s", err)
		}
		var resultMap map[string]interface{}
		err = json.Unmarshal(resBody, &resultMap)
		if err != nil {
			return "", fmt.Errorf("client: could not read json: %s", err)
		}

		if jobState, ok := resultMap["jobState"]; ok {
			v := fmt.Sprintf("%v", jobState)
			// possible results
			//"NOT_SUBMITTED, STARTING, RUNNING, COMPLETED, CANCELLED, FAILED, CANCELLATION_REQUESTED, PLANNING, PENDING, METADATA_RETRIEVAL, QUEUED, ENGINE_START, EXECUTION_PLANNING, INVALID_STATE
			if v == "COMPLETED" || v == "CANCELLED" || v == "FAILED" || v == "INVALID_STATE" || v == "CANCELLATION_REQUESTED" || v == "" {
				return v, nil
			}
			token := fmt.Sprintf("%v", v)
			if token == "" {
				return "", errors.New("blank id cannot proceed")
			}
			lastState = v
		} else {
			return "", fmt.Errorf("invalid result body for id %v: %#v", id, resultMap)
		}
	}
	return lastState, fmt.Errorf("query timed out after %v. state was %v", h.queryTimeoutDuration, lastState)
}

// NewHTTPEngine creates the object capable of making calls against the Dremio REST API
func NewHTTPEngine(a ProtocolArgs) (*HTTPProtocolEngine, error) {
	client, token, err := authenticateHTTP(a)
	if err != nil {
		return &HTTPProtocolEngine{}, err
	}
	intervalsPerSecond := 2.0
	sleepTimeSeconds := 1.0 / intervalsPerSecond
	totalIterations := int(a.Timeout.Seconds() * intervalsPerSecond)
	slog.Debug("initializing http status check parameters", "sleep_time_second", sleepTimeSeconds, "max_status_check_calls", totalIterations)
	return &HTTPProtocolEngine{
		token:                fmt.Sprintf("Bearer %v", token),
		queryURL:             fmt.Sprintf("%v/projects/%v/sql", a.URL, a.CloudProjectID),
		queryJobsURL:         fmt.Sprintf("%v/projects/%v/job", a.URL, a.CloudProjectID),
		client:               client,
		queryTimeoutDuration: a.Timeout,
		totalRetries:         totalIterations,
		sleepTimeSeconds:     sleepTimeSeconds,
	}, nil
}

func authenticateHTTP(a ProtocolArgs) (http.Client, string, error) {
	client := http.Client{
		Timeout: 30 * time.Second,
	}
	return client, a.Password, nil
}

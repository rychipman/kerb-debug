// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package connstring_test

import (
	"encoding/json"
	"io/ioutil"
	"path"
	"testing"

	"github.com/10gen/mongo-go-driver/mongo/connstring"
	"github.com/10gen/mongo-go-driver/mongo/internal/testutil/helpers"
	"github.com/stretchr/testify/require"
)

type host struct {
	Type string
	Host string
	Port json.Number
}

type auth struct {
	Username string
	Password *string
	DB       string
}

type testCase struct {
	Description string
	URI         string
	Valid       bool
	Hosts       []host
	Auth        *auth
	Options     map[string]interface{}
}

type testContainer struct {
	Tests []testCase
}

const testsDir string = "../../data/connection-string/"

func (h *host) toString() string {
	switch h.Type {
	case "unix":
		return h.Host
	case "ip_literal":
		if len(h.Port) == 0 {
			return "[" + h.Host + "]"
		} else {
			return "[" + h.Host + "]" + ":" + string(h.Port)
		}
	case "ipv4":
		fallthrough
	case "hostname":
		if len(h.Port) == 0 {
			return h.Host
		} else {
			return h.Host + ":" + string(h.Port)
		}
	}

	return ""
}

func hostsToStrings(hosts []host) []string {
	out := make([]string, len(hosts))

	for i, host := range hosts {
		out[i] = host.toString()
	}

	return out
}

func runTestsInFile(t *testing.T, filename string) {
	filepath := path.Join(testsDir, filename)
	content, err := ioutil.ReadFile(filepath)
	require.NoError(t, err)

	var container testContainer
	require.NoError(t, json.Unmarshal(content, &container))

	// Remove ".json" from filename.
	filename = filename[:len(filename)-5]

	for _, testCase := range container.Tests {
		runTest(t, filename, &testCase)
	}
}

func runTest(t *testing.T, filename string, test *testCase) {
	testName := filename + ":" + test.Description

	t.Run(testName, func(t *testing.T) {
		cs, err := connstring.Parse(test.URI)
		if test.Valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			return
		}

		require.Equal(t, test.URI, cs.Original)
		require.Equal(t, hostsToStrings(test.Hosts), cs.Hosts)

		if test.Auth != nil {
			require.Equal(t, test.Auth.Username, cs.Username)

			if test.Auth.Password == nil {
				require.False(t, cs.PasswordSet)
			} else {
				require.True(t, cs.PasswordSet)
				require.Equal(t, *test.Auth.Password, cs.Password)
			}

			require.Equal(t, test.Auth.DB, cs.Database)
		}

		// Check that all options are present.
		testhelpers.VerifyConnStringOptions(t, cs, test.Options)

		// Check that non-present options are unset. This will be redundant with the above checks
		// for options that are present.
		var ok bool

		_, ok = test.Options["maxconnsperhost"]
		require.Equal(t, ok, cs.MaxConnsPerHostSet)

		_, ok = test.Options["maxidleconnsperhost"]
		require.Equal(t, ok, cs.MaxIdleConnsPerHostSet)

		require.Equal(t, test.Auth != nil && test.Auth.Password != nil, cs.PasswordSet)
	})
}

// Test case for all connection string spec tests.
func TestConnStringSpec(t *testing.T) {
	entries, err := ioutil.ReadDir(testsDir)
	require.NoError(t, err)

	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".json" {
			continue
		}

		runTestsInFile(t, entry.Name())
	}
}

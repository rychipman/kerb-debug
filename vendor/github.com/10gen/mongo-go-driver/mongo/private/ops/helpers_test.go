// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package ops_test

import (
	"context"
	"testing"

	"github.com/10gen/mongo-go-driver/bson"
	"github.com/10gen/mongo-go-driver/mongo/internal/testutil"
	"github.com/10gen/mongo-go-driver/mongo/internal/testutil/helpers"
	"github.com/10gen/mongo-go-driver/mongo/private/cluster"
	"github.com/10gen/mongo-go-driver/mongo/private/conn"
	"github.com/10gen/mongo-go-driver/mongo/private/msg"
	. "github.com/10gen/mongo-go-driver/mongo/private/ops"
	"github.com/10gen/mongo-go-driver/mongo/readpref"
	"github.com/stretchr/testify/require"
)

func getServer(t *testing.T) *SelectedServer {

	c := testutil.Cluster(t)

	server, err := c.SelectServer(context.Background(), cluster.WriteSelector(), readpref.Primary())
	require.NoError(t, err)

	return &SelectedServer{
		Server:   server,
		ReadPref: readpref.Primary(),
	}
}

func find(t *testing.T, s Server, batchSize int32) CursorResult {
	findCommand := bson.D{
		bson.NewDocElem("find", testutil.ColName(t)),
	}
	if batchSize != 0 {
		findCommand = append(findCommand, bson.NewDocElem("batchSize", batchSize))
	}
	request := msg.NewCommand(
		msg.NextRequestID(),
		testutil.DBName(t),
		false,
		findCommand,
	)

	c, err := s.Connection(context.Background())
	require.NoError(t, err)
	defer testhelpers.RequireNoErrorOnClose(t, c)

	var result cursorReturningResult

	err = conn.ExecuteCommand(context.Background(), c, request, &result)
	require.NoError(t, err)

	return &result.Cursor
}

type cursorReturningResult struct {
	Cursor firstBatchCursorResult `bson:"cursor"`
}

type firstBatchCursorResult struct {
	FirstBatch []bson.Raw `bson:"firstBatch"`
	NS         string     `bson:"ns"`
	ID         int64      `bson:"id"`
}

func (cursorResult *firstBatchCursorResult) Namespace() Namespace {
	namespace := ParseNamespace(cursorResult.NS)
	return namespace
}

func (cursorResult *firstBatchCursorResult) InitialBatch() []bson.Raw {
	return cursorResult.FirstBatch
}

func (cursorResult *firstBatchCursorResult) CursorID() int64 {
	return cursorResult.ID
}

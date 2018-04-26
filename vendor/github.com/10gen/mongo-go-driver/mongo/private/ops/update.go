// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package ops

import (
	"context"

	"github.com/10gen/mongo-go-driver/bson"
	"github.com/10gen/mongo-go-driver/mongo/internal"
	"github.com/10gen/mongo-go-driver/mongo/options"
	"github.com/10gen/mongo-go-driver/mongo/writeconcern"
)

// Update executes an update command with a given set of update documents and options.
//
// TODO GODRIVER-76: Document which types for interface{} are valid.
func Update(ctx context.Context, s *SelectedServer, ns Namespace, writeConcern *writeconcern.WriteConcern,
	updateDocs []bson.D, result interface{}, options ...options.UpdateOption) error {

	if err := ns.validate(); err != nil {
		return err
	}

	command := bson.D{
		{Name: "update", Value: ns.Collection},
	}

	for _, option := range options {
		switch name := option.UpdateName(); name {
		// upsert, multi, and collation are specified in each update documents
		case "upsert":
			fallthrough
		case "multi":
			fallthrough
		case "collation":
			for i, doc := range updateDocs {
				doc.AppendElem(name, option.UpdateValue())
				updateDocs[i] = doc
			}

		// other options are specified in the top-level command document
		default:
			command.AppendElem(name, option.UpdateValue())
		}
	}

	command.AppendElem("updates", updateDocs)

	if writeConcern != nil {
		command.AppendElem("writeConcern", writeConcern)
	}

	err := runMustUsePrimary(ctx, s, ns.DB, command, result)
	if err != nil {
		return internal.WrapError(err, "failed to execute update")
	}

	return nil
}

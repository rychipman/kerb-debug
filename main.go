package main

import (
	"context"
	"fmt"
	"os"

	"github.com/10gen/mongo-go-driver/mongo/connstring"
	"github.com/10gen/mongo-go-driver/mongo/model"
	"github.com/10gen/mongo-go-driver/mongo/private/auth"
	"github.com/10gen/mongo-go-driver/mongo/private/cluster"
	"github.com/10gen/mongo-go-driver/mongo/private/conn"
	"github.com/10gen/mongo-go-driver/mongo/private/server"
	"github.com/10gen/mongo-go-driver/mongo/readpref"
)

func main() {
	err := testKerb()
	if err != nil {
		fmt.Printf("kerb test failed: %v\n", err)
		os.Exit(1)
	}
}

func testKerb() error {

	uri := "mongodb://localhost"

	cs, err := connstring.Parse(uri)
	if err != nil {
		return err
	}

	if err = validateConnString(cs); err != nil {
		return err
	}

	rp, err := getReadPreference(cs)
	if err != nil {
		return err
	}

	clusterOpts := []cluster.Option{
		// before WithConnString makes these
		// the defaults...
		cluster.WithServerOptions(
			server.WithMaxConnections(0),       // no upper limit per host
			server.WithMaxIdleConnections(100), // pool 100 connections per host
			server.WithConnectionOptions(
				conn.WithAppName("kerb-test"),
				conn.WithLifeTimeout(0),
				conn.WithIdleTimeout(0),
			),
		),
		cluster.WithConnString(cs),
	}

	c, err := cluster.New(clusterOpts...)
	if err != nil {
		return err
	}

	if c != nil {
		fmt.Println("got non-nil cluster")
	}

	ctx := context.Background()
	selector := readpref.Selector(rp)
	server, err := c.SelectServer(ctx, selector, rp)
	if err != nil {
		return err
	}

	if server != nil {
		fmt.Println("got non-nil server")
	}

	conn, err := server.Connection(ctx)
	if err != nil {
		return err
	}

	if conn != nil {
		fmt.Println("got non-nil conn")
	}

	authCred := &auth.Cred{
		Source:      "admin",
		Username:    "kerb-test",
		Password:    "kerb-test",
		PasswordSet: true,
	}

	authenticator, err := auth.CreateAuthenticator("SCRAM-SHA-1", authCred)
	if err != nil {
		return err
	}

	err = authenticator.Auth(ctx, conn)
	if err != nil {
		return err
	}

	fmt.Println("successfully authed connection")

	return nil
}

func validateConnString(cs connstring.ConnString) error {
	if cs.Username != "" || cs.PasswordSet ||
		cs.AuthSource != "" || cs.AuthMechanism != "" ||
		len(cs.AuthMechanismProperties) != 0 {

		return fmt.Errorf("--mongo-uri may not contain any authentication information")
	}
	if cs.Database != "" {
		return fmt.Errorf("--mongo-uri may not contain database name")
	}

	return nil
}

func getReadPreference(cs connstring.ConnString) (*readpref.ReadPref, error) {
	var err error
	mode := readpref.PrimaryMode
	if cs.ReadPreference != "" {
		mode, err = readpref.ModeFromString(cs.ReadPreference)
		if err != nil {
			return nil, err
		}
	}

	if len(cs.ReadPreferenceTagSets) > 0 {
		tagSets := model.NewTagSetsFromMaps(cs.ReadPreferenceTagSets)
		return readpref.New(mode, readpref.WithTagSets(tagSets...))
	}

	return readpref.New(mode)
}

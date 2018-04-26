package main

import (
	"context"
	"fmt"
	"time"

	"github.com/10gen/mongo-go-driver/bson"
	"github.com/10gen/mongo-go-driver/mongo/connstring"
	"github.com/10gen/mongo-go-driver/mongo/model"
	"github.com/10gen/mongo-go-driver/mongo/private/auth"
	"github.com/10gen/mongo-go-driver/mongo/private/cluster"
	"github.com/10gen/mongo-go-driver/mongo/private/conn"
	"github.com/10gen/mongo-go-driver/mongo/private/ops"
	"github.com/10gen/mongo-go-driver/mongo/private/server"
	"github.com/10gen/mongo-go-driver/mongo/readpref"
)

func main() {
	uri := "mongodb://ldaptest.10gen.cc:27017"
	username := "drivers@LDAPTEST.10GEN.CC"
	password := "powerbook17"

	fmt.Println()
	err := testKerb(uri, username, password)
	if err != nil {
		fmt.Printf("kerb test failed: %v\n", err)
	}

	fmt.Println()
	err = driverTestKerb(uri, username, password)
	if err != nil {
		fmt.Printf("driver's kerb test failed: %v\n", err)
	}
}

func testKerb(uri, username, password string) error {

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
		Source:      "$external",
		Username:    username,
		Password:    password,
		PasswordSet: true,
	}

	authenticator, err := auth.CreateAuthenticator("GSSAPI", authCred)
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

func driverTestKerb(uri, username, password string) error {

	cs, err := connstring.Parse(uri)
	if err != nil {
		return err
	}

	c, err := cluster.New(
		cluster.WithConnString(cs),
	)
	if err != nil {
		return err
	}

	ctx := context.Background()
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	s, err := c.SelectServer(timeoutCtx, cluster.WriteSelector(), readpref.Primary())
	if err != nil {
		return fmt.Errorf("%v: %v", err, c.Model().Servers[0].LastError)
	}

	dbname := cs.Database
	if dbname == "" {
		dbname = "test"
	}

	var result bson.D
	err = ops.Run(
		ctx,
		&ops.SelectedServer{
			Server:   s,
			ReadPref: readpref.Primary(),
		},
		dbname,
		bson.D{{"count", "test"}},
		&result)
	if err != nil {
		return fmt.Errorf("failed executing count command on %s.%s: %v", dbname, "test", err)
	}

	fmt.Println(result)

	return nil
}

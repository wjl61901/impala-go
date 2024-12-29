# Golang Apache Impala Driver 

**Apache Impala driver for Go's [database/sql](https://golang.org/pkg/database/sql) package**

This driver started as a fork of [github.com/bippio/go-impala](https://github.com/bippio/go-impala),
which hasn't been updated in over four years and appears to be abandoned.
Several issues have been fixed since.

![Tests](https://github.com/sclgo/impala-go/actions/workflows/ci.yml/badge.svg)

## Install

Add `impala-go` to your Go module:

```bash
go get github.com/sclgo/impala-go
```

Alternatively, see below how to use as a CLI.

## Connection Parameters and DSN

The connection string uses a URL format: impala://username:password@host:port?param1=value&param2=value

### Parameters:

* `auth` - string. Authentication mode. Supported values: "noauth", "ldap"
* `tls` - boolean. Enable TLS
* `ca-cert` - The file that contains the public key certificate of the CA that signed the impala certificate
* `batch-size` - integer value (default: 1024). Maximum number of rows fetched per request
* `buffer-size`- in bytes (default: 4096); Buffer size for the Thrift transport 
* `mem-limit` - string value (example: 3m); Memory limit for query 	

A string of this format can be constructed using the URL type in the net/url package.

```go
  query := url.Values{}
  query.Add("auth", "ldap")

  u := &url.URL{
      Scheme:   "impala",
      User:     url.UserPassword(username, password),
      Host:     net.JoinHostPort(hostname, port),
      RawQuery: query.Encode(),
  }
  db, err := sql.Open("impala", u.String())
```

Also, you can bypass string-base data source name by using sql.OpenDB:

```go
  opts := impala.DefaultOptions
  opts.Host = hostname
  opts.UseLDAP = true
  opts.Username = username
  opts.Password = password

  connector := impala.NewConnector(&opts)
  db := sql.OpenDB(connector)
```


## Try out with as a CLI

`impala-go` is compatible with [xo/usql](https://github.com/xo/usql) - the universal SQL CLI, 
inspired by [psql](https://www.postgresql.org/docs/current/app-psql.html). 
Since `impala-go` is not yet included in `usql` by default, you need a Go 1.21+ runtime to build a bundle
from source with [usqlgen](https://github.com/sclgo/usqlgen).

To build the CLI, run:

```bash
go run github.com/sclgo/usqlgen@latest build --import github.com/sclgo/impala-go -- -tags no_base
```

To connect to Impala in interactive mode, run:

```bash
./usql impala:DSN
```

In that command, DSN is a connection string in the format shown above. Note that the DSN itself starts with `impala:`.

For example, to run `show databases` in an Impala instance on localhost, use:

```bash
./usql impala:impala://localhost -c "show databases"
```

## Example Go code

```go
package main

// Simple program to list databases and the tables

import (
	"context"
	"database/sql"
	"log"

	"github.com/sclgo/impala-go"
)

func main() {

	opts := impala.DefaultOptions

	opts.Host = "<impala host>"
	opts.Port = "21050"

	// enable LDAP authentication:
	opts.UseLDAP = true
	opts.Username = "<ldap username>"
	opts.Password = "<ldap password>"

	// enable TLS
	opts.UseTLS = true
	opts.CACertPath = "/path/to/cacert"

	connector := impala.NewConnector(&opts)
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx := context.Background()

	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		log.Fatal(err)
	}

	r := struct {
		name    string
		comment string
	}{}

	databases := make([]string, 0) // databases will contain all the DBs to enumerate later
	for rows.Next() {
		if err := rows.Scan(&r.name, &r.comment); err != nil {
			log.Fatal(err)
		}
		databases = append(databases, r.name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("List of Databases", databases)

	stmt, err := db.PrepareContext(ctx, "SHOW TABLES IN ?")
	if err != nil {
		log.Fatal(err)
	}

	tbl := struct {
		name string
	}{}

	for _, d := range databases {
		rows, err := stmt.QueryContext(ctx, d)
		if err != nil {
			log.Printf("error in querying database %s: %s", d, err.Error())
			continue
		}

		tables := make([]string, 0)
		for rows.Next() {
			if err := rows.Scan(&tbl.name); err != nil {
				log.Println(err)
				continue
			}
			tables = append(tables, tbl.name)
		}
		log.Printf("List of Tables in Database %s: %v\n", d, tables)
	}
}

```

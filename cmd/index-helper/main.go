package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/sters/index-helper/indexhelper/mysql"
)

func main() {
	var (
		adapter  string
		user     string
		password string
		host     string
	)
	flag.StringVar(&adapter, "adapter", "", "")
	flag.StringVar(&user, "user", "", "")
	flag.StringVar(&password, "password", "", "")
	flag.StringVar(&host, "host", "", "")
	flag.Parse()

	if adapter != "mysql" {
		panic(fmt.Sprintf("not supported adapter: %s", adapter))
	}

	db, err := mysql.Open(user, password, host)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.FetchIndexInfo(ctx); err != nil {
		panic(err)
	}

	// result := indexhelper.FindOverWrapIndex(index)

	// spew.Dump(result)
}

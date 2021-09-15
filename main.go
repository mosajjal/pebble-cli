package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/spf13/cobra"
)

var BATCH_SIZE = 1000000

func handleInterrupt(txn *badger.WriteBatch, db *badger.DB) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Println("SIGINT received")
			println("committing..")
			if err := txn.Flush(); err != nil {
				log.Printf("%v", err)
			}
			db.Close()
			time.Sleep(1 * time.Second)
			os.Exit(0)
		}
	}()
}
func index(cmd *cobra.Command, args []string) {
	db, err := badger.Open(badger.DefaultOptions(cmd.Flag("path").Value.String()))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	wb := db.NewWriteBatch()
	defer wb.Cancel()
	handleInterrupt(wb, db)

	scanner := bufio.NewScanner(os.Stdin)
	c := 0
	startTime := time.Now()
	for scanner.Scan() {
		c++
		i := scanner.Text()
		line := strings.SplitN(i, ",", 2) //split the lines to key value by comma
		if len(line) != 2 {               // deal with single column data
			line = append(line, "")
		}
		wb.Set([]byte(line[0]), []byte(line[1]))
		if c%BATCH_SIZE == 0 {
			log.Printf("committing: %v, right around %s\n", c, i)
			log.Printf("ingestion rate: %f", float64(c)/time.Since(startTime).Seconds())
		}
	}
	if err := wb.Flush(); err != nil {
		log.Printf("%v", err)
	}
	println("Index finished")
}

func query(cmd *cobra.Command, args []string) {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	db, err := badger.Open(badger.DefaultOptions(cmd.Flag("path").Value.String()))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		lineInput := scanner.Text()

		fmt.Printf("querying %s\n", lineInput)
		_, err := txn.Get([]byte(lineInput))
		if err != nil {
			fmt.Printf("FAILED: %v\n", lineInput)
		} else {
			fmt.Printf("FOUND: %v\n", lineInput)
		}
	}
}

func remove(cmd *cobra.Command, args []string) {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	db, err := badger.Open(badger.DefaultOptions(cmd.Flag("path").Value.String()))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	lineInput := ""
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		lineInput = scanner.Text()

		err = txn.Delete([]byte(lineInput))
		txn.Commit()
		if err != nil {
			fmt.Printf("ERROR: %v: %v\n", err, lineInput)
		} else {
			fmt.Printf("SUCCESS: %v\n", lineInput)
		}
	}
}

func dump(cmd *cobra.Command, args []string) {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	db, err := badger.Open(badger.DefaultOptions(cmd.Flag("path").Value.String()))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		err := item.Value(func(v []byte) error {
			fmt.Printf("%s,%s\n", k, v)
			return nil
		})
		if err != nil {
			continue
		}
	}
}

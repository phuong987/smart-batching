package main

import (
	"database/sql"
	"flag"
	"fmt"
	"sort"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"smartbatching"
)

var (
	dbAddr       = flag.String("a", "user:pw@tcp(127.0.0.1:3306)/mysql", "mysql address")
	nThread      = flag.Int("n", 1000, "number of thread")
	nRequest     = flag.Int("r", 1, "number of request per thread")
	tblName      = "balance"
	keyField     = "acc"
	balanceField = "amount"
)

type balanceHotspot struct {
	db *sql.DB
}

func (b *balanceHotspot) Do(key string, datas []interface{}) []interface{} {
	tx, err := b.db.Begin()
	var balance int64 = 0
	fail := func(err error) []interface{} {
		if err != nil {
			panic(err)
		}
		return datas
	}
	if err != nil {
		return fail(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone { // ErrTxDone returned if the transaction has already been committed or rolled back
			fmt.Printf("Failed to rollback transaction: %v", err)
		}
	}()

	if err = tx.QueryRow(fmt.Sprintf("SELECT %s from %s where %s=? for update", balanceField, tblName, keyField), key).Scan(&balance); err != nil {
		return fail(err)
	}

	for i, data := range datas {
		amount := data.(int64)
		if amount <= balance {
			datas[i] = true
			balance -= amount
		} else {
			datas[i] = false
		}

	}
	if _, err = tx.Exec(fmt.Sprintf("Update %s  SET %s=? WHERE %s=?", tblName, balanceField, keyField), balance, key); err != nil {
		return fail(err)
	}

	if err = tx.Commit(); err != nil {
		return fail(err)
	}
	return datas
}
func main() {
	flag.Parse()
	keyTest := "A"
	db, err := sql.Open("mysql", *dbAddr)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			fmt.Printf("Error closing database: %v", err)
		}
	}()

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(%s VARCHAR(256) PRIMARY KEY, %s BIGINT(20))", tblName, keyField, balanceField)
	if _, err = db.Exec(query); err != nil {
		panic(err)
	}
	query = fmt.Sprintf("INSERT IGNORE INTO %s VALUES('%s',1000000)", tblName, keyTest)
	if _, err = db.Exec(query); err != nil {
		fmt.Printf("Error %v\n", err)
		panic(err)
	}
	n := *nRequest * *nThread
	durations := make([]int, 0, n)
	muDurations := sync.Mutex{}
	batch := smartbatching.NewSmartBatching(&balanceHotspot{db})

	wg := sync.WaitGroup{}
	t0 := time.Now()
	for t := 0; t < *nThread; t++ {
		wg.Add(1)
		go func() {
			for r := 0; r < *nRequest; r++ {
				t1 := time.Now()
				batch.Add(keyTest, int64(1))
				muDurations.Lock()
				durations = append(durations, int(time.Since(t1).Milliseconds()))
				muDurations.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	sort.Ints(durations)

	fmt.Printf("TPS: %d\n", int(float64(n)/float64(time.Since(t0).Seconds())))
	percentiles := []int{99, 95, 90, 75, 50}
	for _, percentile := range percentiles {
		fmt.Printf("P%d: %d(ms)\n", percentile, durations[n*percentile/100])
	}

}

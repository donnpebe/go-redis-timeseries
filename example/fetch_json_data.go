package main

import (
	"fmt"
	"time"

	"github.com/donnpebe/go-redis-timeseries"
	"github.com/garyburd/redigo/redis"
)

type Data struct {
	Author  string `json:"author"`
	Message string `json:"mesage"`
}

func main() {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ts := timeseries.NewTimeSeries("dump::device1", 1*time.Second, conn)

	now := time.Now()
	fmt.Printf("Adding data points...\n\n")
	data := Data{"author 1", "message 1"}
	err = ts.Add(&data, true, now)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Get from %v...\n\n", now)

	dat := new(Data)
	if err = ts.Fetch(now, true, dat); err != nil {
		panic(err)
	}

	fmt.Println("Records")
	fmt.Println("=======")

	fmt.Printf("Author: %s, Message: %s\n", dat.Author, dat.Message)

}

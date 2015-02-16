package main

import (
	"fmt"
	"time"

	"github.com/donnpebe/go-redis-timeseries"
	"github.com/garyburd/redigo/redis"
)

func main() {
	conn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ts := timeseries.NewTimeSeries("dump::device1", 1*time.Second, conn)

	fmt.Printf("Adding data points...\n\n")

	now := time.Now()
	err = ts.Add("data10000", now)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Get from %v...\n\n", now)

	var str string
	if err = ts.Fetch(now, &str); err != nil {
		panic(err)
	}

	fmt.Println("Records")
	fmt.Println("=======")

	fmt.Println(str)

}

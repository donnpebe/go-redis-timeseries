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
	for i := 0; i < 300; i++ {
		data := &Data{fmt.Sprintf("Author %d", i+1), fmt.Sprintf("Message %d", i+1)}
		tm := now.Add(time.Duration(i) * 10 * time.Millisecond)
		err = ts.Add(data, tm)
		if err != nil {
			panic(err)
		}
	}

	begin := now.Add(1 * time.Second)
	end := now.Add(2500 * time.Millisecond)

	fmt.Printf("Get range from %v to %v...\n\n", begin, end)

	var results []*Data
	if err = ts.FetchRange(begin, end, &results); err != nil {
		panic(err)
	}

	fmt.Println("Records")
	fmt.Println("=======")
	fmt.Println(len(results))
	for _, v := range results {
		fmt.Printf("Author: %v, Message: %v\n", v.Author, v.Message)
	}
}

package main

import (
	"fmt"
	"strconv"
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

	now := time.Now()
	fmt.Printf("Adding data points...\n\n")
	for i := 0; i < 300; i++ {
		tm := now.Add(time.Duration(i) * 10 * time.Millisecond)
		err = ts.Add("data"+strconv.Itoa(i), tm)
		if err != nil {
			panic(err)
		}
	}

	begin := now.Add(1 * time.Second)
	end := now.Add(2 * time.Second)

	fmt.Printf("Get range from %v to %v...\n\n", begin, end)

	var strs []string
	if err = ts.FetchRange(begin, end, &strs); err != nil {
		panic(err)
	}

	fmt.Println("Records")
	fmt.Println("=======")

	for i, v := range strs {
		fmt.Println(i, v)
	}

}

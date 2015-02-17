# go-redis-timeseries

Save time-series data to redis.
This code is inspired by [https://github.com/antirez/redis-timeseries/pull/1/files](https://github.com/antirez/redis-timeseries/pull/1/files).

## Usage

See Example.

Remember, this implementation is using Redis Sorted Set, so if you save an exact data more than one, sorted set only have a single copy of this data. One solution to overcome this limitation is to include timestamp in your data.

## Disclaimer

The code in this repo has not been fully tested. Use at your own risk.

## License

[MIT Public License](https://github.com/donnpebe/go-redis-timeseries/blob/master/LICENSE)
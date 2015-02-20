package timeseries

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/alecthomas/binary"
	"github.com/garyburd/redigo/redis"
)

var ErrNotFound = errors.New("record not found")

// TimeSeries is use to save time series data to redis
type TimeSeries struct {
	sync.Mutex
	prefix     string
	timestep   time.Duration
	expiration time.Duration
	db         redis.Conn
}

// NewTimeSeries create new TimeSeries
func NewTimeSeries(prefix string, timestep time.Duration, exp time.Duration, db redis.Conn) *TimeSeries {
	return &TimeSeries{
		prefix:     prefix,
		timestep:   timestep,
		expiration: exp,
		db:         db,
	}
}

// Add add data to timeseries db
func (t *TimeSeries) Add(data interface{}, tm ...time.Time) (err error) {
	inputTm := time.Now()
	if len(tm) > 0 {
		inputTm = tm[0]
	}
	var dataBytes []byte
	if dataBytes, err = binary.Marshal(data); err != nil {
		return
	}

	t.Lock()
	t.db.Send("MULTI")
	t.db.Send("ZADD", t.key(inputTm), inputTm.UnixNano(), dataBytes)
	if t.expiration > 0 {
		sc := redis.NewScript(2, "local ex = redis.pcall('zcard', KEYS[1]) \n if ex == 1 then return redis.call('expire', KEYS[1], KEYS[2]) end")
		sc.Send(t.db, t.key(inputTm), int64(t.expiration.Seconds()))
	}
	_, err = t.db.Do("EXEC")

	t.Unlock()

	return
}

// Fetch get value one time
func (t *TimeSeries) Fetch(tm time.Time, dest interface{}) (err error) {
	d := reflect.ValueOf(dest)
	if d.Kind() != reflect.Ptr || d.IsNil() {
		return errors.New("Fetch value must be non-nil pointer")
	}

	tmi := tm.UnixNano()

	t.Lock()
	value, err := redis.Strings(t.db.Do("ZRANGEBYSCORE", t.key(tm), tmi, tmi))
	t.Unlock()

	if err != nil {
		return
	}

	if len(value) == 0 {
		return ErrNotFound
	}

	return binary.Unmarshal([]byte(value[0]), dest)
}

// FetchRange fetch data from the begin time to end time
func (t *TimeSeries) FetchRange(begin, end time.Time, dest interface{}) (err error) {
	if begin.After(end) {
		return errors.New("Begin time value must be less than end time value")
	}

	d := reflect.ValueOf(dest)
	if d.Kind() != reflect.Ptr || d.IsNil() {
		return errors.New("Fetch value must be non-nil pointer")
	}
	d = d.Elem()
	if d.Kind() != reflect.Slice {
		return errors.New("Fetch value must be pointer to slice")
	}

	typ := d.Type().Elem()

	isPtr := false
	if typ.Kind() == reflect.Ptr {
		isPtr = true
		typ = typ.Elem()
	}

	tmb := begin.UnixNano()
	tme := end.UnixNano()

	numOfKey := (tme - tmb) / int64(t.timestep.Nanoseconds())

	t.Lock()
	for i := int64(0); i <= numOfKey; i++ {
		key := t.key(begin.Add(time.Duration(time.Duration(i) * t.timestep)))
		t.db.Send("ZRANGEBYSCORE", key, tmb, tme)
	}
	t.db.Flush()

	dumpData := make([][]string, numOfKey+1)
	rcCount := 0
	for i := int64(0); i <= numOfKey; i++ {
		dumpData[i], err = redis.Strings(t.db.Receive())
		if err != nil {
			return
		}
		rcCount += len(dumpData[i])
	}

	t.Unlock()

	ensureLen(d, rcCount)
	i := 0
	for _, v := range dumpData {
		for _, r := range v {
			d := d.Index(i)
			var val interface{}
			if isPtr {
				if d.IsNil() {
					d.Set(reflect.New(typ))
				}
				val = d.Interface()
			} else {
				val = d.Addr().Interface()
			}

			binary.Unmarshal([]byte(r), val)
			i++
		}
	}

	return
}

func (t *TimeSeries) key(tm time.Time) string {
	tmi := tm.UnixNano()
	return fmt.Sprintf("%s::%d", t.prefix, t.normalizeTimeInt64(tmi))
}

func (t *TimeSeries) normalizeTimeInt64(tm int64) int64 {
	return tm - (tm % int64(t.timestep.Nanoseconds()))
}

func ensureLen(d reflect.Value, n int) {
	if n > d.Cap() {
		d.Set(reflect.MakeSlice(d.Type(), n, n))
	} else {
		d.SetLen(n)
	}
}

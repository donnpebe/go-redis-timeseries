package timeseries

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// TimeSeries is use to save time series data to redis
type TimeSeries struct {
	sync.Mutex
	prefix   string
	timestep time.Duration
	db       redis.Conn
}

// NewTimeSeries create new TimeSeries
func NewTimeSeries(prefix string, timestep time.Duration, db redis.Conn) *TimeSeries {
	return &TimeSeries{
		prefix:   prefix,
		timestep: timestep,
		db:       db,
	}
}

// Add add data to timeseries db
func (t *TimeSeries) Add(data interface{}, encodeJSON bool, tm ...time.Time) (err error) {
	inputTm := time.Now()
	if len(tm) > 0 {
		inputTm = tm[0]
	}
	var dataBytes []byte
	if encodeJSON {
		if dataBytes, err = json.Marshal(data); err != nil {
			return
		}
	} else {
		str, ok := data.(string)
		if !ok {
			return errors.New("data not string, need to set encodeJSON to be truthy")
		}
		dataBytes = []byte(str)
	}

	t.Lock()
	_, err = t.db.Do("ZADD", t.key(inputTm), inputTm.UnixNano(), dataBytes)
	t.Unlock()

	return
}

// Fetch get value one time
func (t *TimeSeries) Fetch(tm time.Time, decodeJSON bool, dest interface{}) (err error) {
	d := reflect.ValueOf(dest)
	if d.Kind() != reflect.Ptr || d.IsNil() {
		return errors.New("Fetch value must be non-nil pointer")
	}
	elem := d.Elem()
	if !decodeJSON {
		if elem.Kind() != reflect.String {
			return errors.New("Fetch value must be pointer to string")
		}
	}

	tmi := tm.UnixNano()

	t.Lock()
	value, err := redis.Strings(t.db.Do("ZRANGEBYSCORE", t.key(tm), tmi, tmi))

	t.Unlock()

	if err != nil {
		return
	}

	if decodeJSON {
		return json.Unmarshal([]byte(value[0]), dest)
	}

	elem.SetString(value[0])
	return
}

// FetchRange fetch data from the begin time to end time
func (t *TimeSeries) FetchRange(begin, end time.Time, decodeJSON bool, dest interface{}) (err error) {
	d := reflect.ValueOf(dest)
	if d.Kind() != reflect.Ptr || d.IsNil() {
		return errors.New("Fetch value must be non-nil pointer")
	}
	d = d.Elem()
	if d.Kind() != reflect.Slice {
		return errors.New("Fetch value must be pointer to slice")
	}

	typ := d.Type().Elem()

	if !decodeJSON && typ.Kind() != reflect.String {
		return errors.New("Fetch value must be pointer to string slice")
	}

	isPtr := false
	if decodeJSON && typ.Kind() == reflect.Ptr {
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
			if decodeJSON {
				var val interface{}
				if isPtr {
					if d.IsNil() {
						d.Set(reflect.New(typ))
					}
					val = d.Interface()
				} else {
					val = d.Addr().Interface()
				}
				json.Unmarshal([]byte(r), val)
			} else {
				d.SetString(r)
			}
			i++
		}
	}

	return
}

func (t *TimeSeries) key(tm time.Time) string {
	tmi := tm.UnixNano()
	return fmt.Sprintf("%s::ts::%d", t.prefix, t.normalizeTimeInt64(tmi))
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

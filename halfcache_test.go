package halfcache

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHalfCache(t *testing.T) {
	var (
		callCount = 0
		lastKeys  []string
	)

	directMultiGet := func(keys ...string) (map[string][]byte, error) {
		time.Sleep(100 * time.Millisecond)

		callCount++
		lastKeys = keys

		m := map[string][]byte{
			"a": []byte("aaa"),
			"b": []byte("bbb"),
			"c": []byte("ccc"),
		}

		ret := map[string][]byte{}
		for _, key := range keys {
			if value, ok := m[key]; ok {
				ret[key] = value
			}
		}

		return ret, nil
	}

	cache := NewHalfCache(100, 1*time.Second, 5*time.Second, directMultiGet)
	ret, err := cache.MultiGet("a", "c")
	assert.Nil(t, err)
	assert.Equal(t, map[string][]byte{"a": []byte("aaa"), "c": []byte("ccc")}, ret)
	assert.Equal(t, 1, callCount)
	assert.Equal(t, []string{"a", "c"}, lastKeys)

	ret, err = cache.MultiGet("a", "b", "c")
	assert.Nil(t, err)
	assert.Equal(t, map[string][]byte{"a": []byte("aaa"), "b": []byte("bbb"), "c": []byte("ccc")}, ret)
	assert.Equal(t, 2, callCount)
	assert.Equal(t, []string{"b"}, lastKeys)

	// following test must run with multi goroutines.
	if runtime.GOMAXPROCS(0) <= 1 {
		t.Log("test skipped because of runtime.GOMAXPROCS(0) <= 1")
		return
	}

	time.Sleep(2 * time.Second)
	ret, err = cache.MultiGet("a", "b", "c")
	assert.Nil(t, err)
	assert.Equal(t, map[string][]byte{"a": []byte("aaa"), "b": []byte("bbb"), "c": []byte("ccc")}, ret)
	assert.Equal(t, 2, callCount)
	assert.Equal(t, []string{"b"}, lastKeys)

	time.Sleep(1 * time.Second)
	assert.Equal(t, 3, callCount)
	assert.Equal(t, []string{"a", "b", "c"}, lastKeys)
}

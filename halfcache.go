package halfcache

// TODO: add stats

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/coocood/freecache"
)

type Cache interface {
	MultiGet(keys ...string) (map[string][]byte, error)
}

type entity struct {
	POD struct {
		UpdateTimeStamp int64
		FinalTimeStamp  int64
	}

	Data []byte
}

func (e *entity) MarshalBinary() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.LittleEndian, &e.POD)
	if err != nil {
		return nil, fmt.Errorf("entity.MarshalBinary: %v", err)
	}
	_, err = buf.Write(e.Data)
	if err != nil {
		return nil, fmt.Errorf("entity.MarshalBinary: %v", err)
	}
	return buf.Bytes(), nil
}

func (e *entity) UnmarshalBinary(b []byte) error {
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, &e.POD)
	if err != nil {
		return fmt.Errorf("entity.UnmarshalBinary: %v", err)
	}
	e.Data = make([]byte, buf.Len())
	n, err := buf.Read(e.Data)
	if err != nil || n != len(e.Data) {
		return fmt.Errorf("entity.UnmarshalBinary: failed: %v", err)
	}
	return nil
}

type halfCache struct {
	cache *freecache.Cache

	updateTTL time.Duration
	finalTTL  time.Duration

	directMultiGet func(keys ...string) (map[string][]byte, error)
}

func NewHalfCache(size int, updateTTL, finalTTL time.Duration, directMultiGet func(keys ...string) (map[string][]byte, error)) Cache {
	return &halfCache{
		cache:          freecache.NewCache(size),
		updateTTL:      updateTTL,
		finalTTL:       finalTTL,
		directMultiGet: directMultiGet,
	}
}

func (hc *halfCache) extractData(entityBytes []byte) (value []byte, needUpdate bool, valid bool) {
	var entity entity

	needUpdate = false
	valid = true

	err := entity.UnmarshalBinary(entityBytes)
	if err != nil {
		// TODO: track error
		// if error occurs, then it would be invalid data
		return nil, false, false
	}

	now := time.Now().Unix()
	if now > entity.POD.UpdateTimeStamp {
		needUpdate = true
	}
	if now > entity.POD.FinalTimeStamp {
		valid = false
	}

	return entity.Data, needUpdate, valid
}

func (hc *halfCache) tryGetFromCache(keys []string) (ret map[string][]byte, directKeys []string, needBackFill bool) {
	ret = make(map[string][]byte, len(keys))

	for _, key := range keys {
		entityBytes, err := hc.cache.Get([]byte(key))
		if err != nil {
			// must be not found
			directKeys = append(directKeys, key)
			needBackFill = true
			continue
		}

		value, needUpdate, valid := hc.extractData(entityBytes)
		if needUpdate || !valid {
			directKeys = append(directKeys, key)
		}

		if !valid {
			needBackFill = true
		} else {
			ret[key] = value
		}
	}

	return ret, directKeys, needBackFill
}

func (hc *halfCache) updateCache(directRet map[string][]byte) {
	now := time.Now()

	for key, value := range directRet {
		entity := entity{
			Data: value,
			POD: struct {
				UpdateTimeStamp int64
				FinalTimeStamp  int64
			}{
				UpdateTimeStamp: now.Add(hc.updateTTL).Unix(),
				FinalTimeStamp:  now.Add(hc.finalTTL).Unix(),
			},
		}

		entityBytes, err := entity.MarshalBinary()
		if err != nil {
			// TODO: track error
			continue
		}

		// no error is expected.
		err = hc.cache.Set([]byte(key), entityBytes, int(hc.finalTTL))
		if err != nil {
			// TODO: track error
			continue
		}
	}
}

func backFillResult(ret, directRet map[string][]byte) {
	for key, value := range directRet {
		ret[key] = value
	}
}

func (hc *halfCache) MultiGet(keys ...string) (map[string][]byte, error) {
	ret, directKeys, needBackFill := hc.tryGetFromCache(keys)

	if len(directKeys) == 0 {
		return ret, nil
	}

	var (
		wg        sync.WaitGroup
		directErr error
		directRet map[string][]byte
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		directRet, directErr = hc.directMultiGet(directKeys...)
		if directErr != nil {
			return
		}

		hc.updateCache(directRet)
	}()

	if needBackFill {
		wg.Wait()
		if directErr != nil {
			return nil, fmt.Errorf("halfCache: failed to get directly: %v", directErr)
		}
		backFillResult(ret, directRet)
	}

	return ret, nil
}

package ignite

import (
	"context"
	"fmt"
	"time"
)

// CachePeekMode define cache peek modes.
type CachePeekMode uint8

const (
	PeekAll     CachePeekMode = iota // Peeks into all available cache storages.
	PeekNear                         // Peek into near cache only.
	PeekPrimary                      // Peek value from primary copy of partitioned cache only (skip near cache).
	PeekBackup                       // Peek value from backup copies of partitioned cache only (skip near cache).
	PeekOnHeap                       // Peeks value from the on-heap storage only.
	PeekOffHeap                      // Peeks value from the off-heap storage only, without loading off-heap value into cache.
)

const (
	DurationUnchanged time.Duration = -2 // Skip setting and leave as is a duration of specific expiry policy.
	DurationEternal   time.Duration = -1 // Set specific expiry policy as eternal.
	DurationZero      time.Duration = 0  // Set zero duration of specific expiry policy
)

type KeyValue struct {
	Key   interface{}
	Value interface{}
}

const (
	keepBinaryMask    uint8 = 0x01
	transactionalMask uint8 = 0x02
	expiryPolicyMask  uint8 = 0x04
)

const (
	opCacheGet               int16 = 1000
	opCachePut               int16 = 1001
	opCachePutIfAbsent       int16 = 1002
	opCacheGetAll            int16 = 1003
	opCachePutAll            int16 = 1004
	opCacheGetAndPut         int16 = 1005
	opCacheGetAndReplace     int16 = 1006
	opCacheGetAndRemove      int16 = 1007
	opCacheGetAndPutIfAbsent int16 = 1008
	opCacheReplace           int16 = 1009
	opCacheReplaceIfEquals   int16 = 1010
	opCacheContainsKey       int16 = 1011
	opCacheContainsKeys      int16 = 1012
	opCacheClear             int16 = 1013
	opCacheClearKey          int16 = 1014
	opCacheClearKeys         int16 = 1015
	opCacheRemoveKey         int16 = 1016
	opCacheRemoveIfEquals    int16 = 1017
	opCacheRemoveKeys        int16 = 1018
	opCacheRemoveAll         int16 = 1019
	opCacheGetSize           int16 = 1020
	opCacheGetConfiguration  int16 = 1055
)

// ExpiryPolicy determines when entries will expire based on creation, access and modification operations.
type ExpiryPolicy struct {
	creation time.Duration
	access   time.Duration
	update   time.Duration
}

// Creation returns the duration before a newly created cache entry is considered expired.
func (e *ExpiryPolicy) Creation() time.Duration {
	return e.creation
}

// Access returns the duration before an accessed cache entry is considered expired.
func (e *ExpiryPolicy) Access() time.Duration {
	return e.access
}

// Update returns the duration before an updated cache entry is considered expired.
func (e *ExpiryPolicy) Update() time.Duration {
	return e.update
}

// Cache is a facade for all Apache Ignite cache operations:
type Cache struct {
	cli          *Client
	expiryPolicy *ExpiryPolicy
	name         string
	id           int32
}

// Get gets an entry from the cache by key
func (cache *Cache) Get(ctx context.Context, key interface{}) (interface{}, error) {
	if key == nil {
		return nil, fmt.Errorf("nil key")
	}
	var err error = nil
	var ret interface{}
	cache.cli.ch.send(ctx, opCacheGet, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret, err = cache.cli.marsh.unmarshal(ctx, input)
	})
	return ret, err
}

// GetAndPut puts a new value to the cache by key, returns old value if exists or nil.
func (cache *Cache) GetAndPut(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	if key == nil {
		return nil, fmt.Errorf("nil key")
	}
	if value == nil {
		return nil, fmt.Errorf("nil value")
	}
	var ret interface{}
	var err error = nil
	cache.cli.ch.send(ctx, opCacheGetAndPut, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, value)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret, err = cache.cli.marsh.unmarshal(ctx, input)
	})
	return ret, err
}

// GetAndReplace atomically replaces the value if and only if an old mapping exists, returns old value if mapping was set or nil otherwise.
func (cache *Cache) GetAndReplace(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	if key == nil {
		return nil, fmt.Errorf("nil key")
	}
	if value == nil {
		return nil, fmt.Errorf("nil value")
	}
	var ret interface{} = nil
	var err error = nil
	cache.cli.ch.send(ctx, opCacheGetAndReplace, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, value)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret, err = cache.cli.marsh.unmarshal(ctx, input)
	})
	return ret, err
}

// Put puts the value with the specified key.
func (cache *Cache) Put(ctx context.Context, key interface{}, value interface{}) error {
	if key == nil {
		return fmt.Errorf("nil key")
	}
	if value == nil {
		return fmt.Errorf("nil value")
	}
	var err error = nil
	cache.cli.ch.send(ctx, opCachePut, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, value)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
	})
	return err
}

// PutIfAbsent puts the value if there is no mapping by the key, returns true if mapping was set and false otherwise.
func (cache *Cache) PutIfAbsent(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	if key == nil {
		return false, fmt.Errorf("nil key")
	}
	if value == nil {
		return false, fmt.Errorf("nil value")
	}
	var ret bool
	var err error = nil
	cache.cli.ch.send(ctx, opCachePutIfAbsent, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, value)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret = input.ReadBool()
	})
	return ret, err
}

// GetAndPutIfAbsent puts the value if there is no mapping by the key, returns the old value if mapping was set or nil otherwise.
func (cache *Cache) GetAndPutIfAbsent(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	if key == nil {
		return nil, fmt.Errorf("nil key")
	}
	if value == nil {
		return nil, fmt.Errorf("nil value")
	}
	var ret interface{}
	var err error = nil
	cache.cli.ch.send(ctx, opCacheGetAndPutIfAbsent, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, value)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret, err = cache.cli.marsh.unmarshal(ctx, input)
	})
	return ret, err
}

// ContainsKey returns true if key exists, false otherwise.
func (cache *Cache) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	if key == nil {
		return false, fmt.Errorf("nil key")
	}
	var err error = nil
	var ret bool
	cache.cli.ch.send(ctx, opCacheContainsKey, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret = input.ReadBool()
	})
	return ret, err
}

// ContainsKeys returns true if all keys exists, false otherwise.
func (cache *Cache) ContainsKeys(ctx context.Context, keys ...interface{}) (bool, error) {
	if len(keys) == 0 {
		return true, nil
	}
	var err error
	var ret bool
	cache.cli.ch.send(ctx, opCacheContainsKeys, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = writeSequence(output, len(keys), func(output0 BinaryOutputStream, idx int) error {
			key := keys[idx]
			if key == nil {
				return fmt.Errorf("nil key passed to ContainsKeys")
			}
			return cache.cli.marsh.marshal(ctx, output0, key)
		})
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret = input.ReadBool()
	})
	return ret, err
}

// GetAll returns key-value pairs for specified keys.
func (cache *Cache) GetAll(ctx context.Context, keys ...interface{}) ([]KeyValue, error) {
	ret := make([]KeyValue, 0)
	if len(keys) == 0 {
		return ret, nil
	}
	var err error
	cache.cli.ch.send(ctx, opCacheGetAll, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = writeSequence(output, len(keys), func(output0 BinaryOutputStream, idx int) error {
			key := keys[idx]
			if key == nil {
				return fmt.Errorf("nil key passed to GetAll")
			}
			return cache.cli.marsh.marshal(ctx, output0, key)
		})
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		sz := input.ReadInt32()
		if sz == 0 {
			return
		}
		for i := 0; i < int(sz); i++ {
			var key, value interface{}
			key, err0 = cache.cli.marsh.unmarshal(ctx, input)
			if err0 != nil {
				err = err0
				return
			}
			value, err0 = cache.cli.marsh.unmarshal(ctx, input)
			if err0 != nil {
				err = err0
				return
			}
			ret = append(ret, KeyValue{key, value})
		}
	})
	return ret, err
}

// PutAll puts key-value pairs.
func (cache *Cache) PutAll(ctx context.Context, keysAndValues ...KeyValue) error {
	lenData := len(keysAndValues)
	if lenData == 0 {
		return nil
	}
	var err error
	cache.cli.ch.send(ctx, opCachePutAll, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = writeSequence(output, len(keysAndValues), func(output0 BinaryOutputStream, idx int) error {
			kv := keysAndValues[idx]
			if kv.Key == nil {
				return fmt.Errorf("nil key passed to PutAll")
			}
			if kv.Value == nil {
				return fmt.Errorf("nil value passed to PutAll")
			}
			var err0 error
			err0 = cache.cli.marsh.marshal(ctx, output0, kv.Key)
			if err0 != nil {
				return err0
			}
			err0 = cache.cli.marsh.marshal(ctx, output0, kv.Value)
			if err0 != nil {
				return err0
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
	})
	return err
}

// ReplaceIfEquals replaces old value with new value if and only if the old value equals to specified oldValue, returns true if successful.
func (cache *Cache) ReplaceIfEquals(ctx context.Context, key interface{}, oldValue interface{}, newValue interface{}) (bool, error) {
	if key == nil {
		return false, fmt.Errorf("nil key")
	}
	if oldValue == nil {
		return false, fmt.Errorf("nil oldValue")
	}
	if newValue == nil {
		return false, fmt.Errorf("nil newValue")
	}
	var ret bool
	var err error = nil
	cache.cli.ch.send(ctx, opCacheReplaceIfEquals, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, oldValue)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, newValue)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret = input.ReadBool()
	})
	return ret, err
}

// Replace replaces old value with specified value if mapping was set, returns true if successful.
func (cache *Cache) Replace(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	if key == nil {
		return false, fmt.Errorf("nil key")
	}
	if value == nil {
		return false, fmt.Errorf("nil value")
	}
	var ret bool
	var err error = nil
	cache.cli.ch.send(ctx, opCacheReplace, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, value)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret = input.ReadBool()
	})
	return ret, err
}

// Remove removes mapping if it was set, return true if successful.
func (cache *Cache) Remove(ctx context.Context, key interface{}) (bool, error) {
	if key == nil {
		return false, fmt.Errorf("nil key")
	}
	var ret bool
	var err error = nil
	cache.cli.ch.send(ctx, opCacheRemoveKey, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret = input.ReadBool()
	})
	return ret, err
}

// GetAndRemove removes mapping if it was set, return old value if the mapping was set or nil otherwise.
func (cache *Cache) GetAndRemove(ctx context.Context, key interface{}) (interface{}, error) {
	if key == nil {
		return false, fmt.Errorf("nil key")
	}
	var ret interface{} = nil
	var err error = nil
	cache.cli.ch.send(ctx, opCacheGetAndRemove, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret, err = cache.cli.marsh.unmarshal(ctx, input)
	})
	return ret, err
}

// RemoveIfEquals removes old value with new value if and only if the old value equals to specified oldValue, returns true if successful.
func (cache *Cache) RemoveIfEquals(ctx context.Context, key interface{}, oldValue interface{}) (bool, error) {
	if key == nil {
		return false, fmt.Errorf("nil key")
	}
	if oldValue == nil {
		return false, fmt.Errorf("nil oldValue")
	}
	var ret bool
	var err error = nil
	cache.cli.ch.send(ctx, opCacheRemoveIfEquals, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, oldValue)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		ret = input.ReadBool()
	})
	return ret, err
}

// RemoveKeys removes mappings to specified keys.
func (cache *Cache) RemoveKeys(ctx context.Context, keys ...interface{}) error {
	if len(keys) == 0 {
		return nil
	}
	var err error
	cache.cli.ch.send(ctx, opCacheRemoveKeys, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = writeSequence(output, len(keys), func(output0 BinaryOutputStream, idx int) error {
			key := keys[idx]
			if key == nil {
				return fmt.Errorf("nil key passed to RemoveKeys")
			}
			return cache.cli.marsh.marshal(ctx, output0, key)
		})
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
	})
	return err
}

// RemoveAll removes all cache entries.
func (cache *Cache) RemoveAll(ctx context.Context) error {
	var err error
	cache.cli.ch.send(ctx, opCacheRemoveAll, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
	})
	return err
}

// Clear clears mapping to specified key.
func (cache *Cache) Clear(ctx context.Context, key interface{}) error {
	if key == nil {
		return fmt.Errorf("nil key")
	}
	var err error = nil
	cache.cli.ch.send(ctx, opCacheClearKey, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = cache.cli.marsh.marshal(ctx, output, key)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
	})
	return err
}

// ClearKeys clears mappings to specified keys.
func (cache *Cache) ClearKeys(ctx context.Context, keys ...interface{}) error {
	if len(keys) == 0 {
		return nil
	}
	var err error
	cache.cli.ch.send(ctx, opCacheClearKeys, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		err = writeSequence(output, len(keys), func(output0 BinaryOutputStream, idx int) error {
			key := keys[idx]
			if key == nil {
				return fmt.Errorf("nil key passed to ClearKeys")
			}
			return cache.cli.marsh.marshal(ctx, output0, key)
		})
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
	})
	return err
}

// ClearAll clears all cache entries.
func (cache *Cache) ClearAll(ctx context.Context) error {
	var err error
	cache.cli.ch.send(ctx, opCacheClear, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
	})
	return err
}

// Name returns name of the cache.
func (cache *Cache) Name() string {
	return cache.name
}

// WithExpiryPolicy returns wrapper that apply expiry policy on all cache operations. NOTE! Does not create a brand-new cache.
func (cache *Cache) WithExpiryPolicy(creation time.Duration, access time.Duration, update time.Duration) *Cache {
	return &Cache{
		cli: cache.cli,
		expiryPolicy: &ExpiryPolicy{
			creation: creation,
			access:   access,
			update:   update,
		},
		name: cache.name,
		id:   cache.id,
	}
}

// Size returns cache size according to specified peek mode.
func (cache *Cache) Size(ctx context.Context, peekModes ...CachePeekMode) (uint64, error) {
	var size uint64 = 0
	var err error = nil
	cache.cli.ch.send(ctx, opCacheGetSize, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		peekModesSz := int32(len(peekModes))
		output.WriteInt32(peekModesSz)
		for _, peekMode := range peekModes {
			output.WriteInt8(int8(peekMode))
		}
		return nil
	}, func(input BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
			return
		}
		size = input.ReadUInt64()
	})

	return size, err
}

// Configuration returns cache [CacheConfiguration] of this cache.
func (cache *Cache) Configuration(ctx context.Context) (CacheConfiguration, error) {
	var err error
	var cfg CacheConfiguration
	cache.cli.ch.send(ctx, opCacheGetConfiguration, func(output BinaryOutputStream) error {
		err = cache.writeCacheInfo(cache.cli.ch.protocolContext(), output)
		if err != nil {
			return err
		}
		return nil
	}, func(output BinaryInputStream, err0 error) {
		if err0 != nil {
			err = err0
		} else {
			cfg, err = unmarshalCacheConfiguration(ctx, cache.cli.marsh, output)
		}
	})

	return cfg, err
}

func (cache *Cache) writeCacheInfo(protoCtx *ProtocolContext, output BinaryOutputStream) error {
	output.WriteInt32(cache.id)
	var flag = keepBinaryMask
	if cache.expiryPolicy != nil {
		if !protoCtx.SupportsExpiryPolicy() {
			return fmt.Errorf("expiry policies are not supported for protocol %v", protoCtx.Version())
		}
		flag |= expiryPolicyMask
	}
	output.WriteUInt8(flag)
	if flag&expiryPolicyMask != 0 {
		output.WriteInt64(durationToMillis(cache.expiryPolicy.Creation()))
		output.WriteInt64(durationToMillis(cache.expiryPolicy.Update()))
		output.WriteInt64(durationToMillis(cache.expiryPolicy.Access()))
	}
	return nil
}

func durationToMillis(dur time.Duration) int64 {
	if dur > 0 {
		return dur.Milliseconds()
	} else if dur <= DurationUnchanged {
		return int64(DurationUnchanged)
	} else {
		return int64(dur)
	}
}

func millisToDuration(millis int64) time.Duration {
	if millis > 0 {
		return time.Duration(millis) * time.Millisecond
	} else {
		return time.Duration(millis)
	}
}

// Package ignite provides a go driver for Apache Ignite.
//
// # Introduction
//
// Basic usage of the apache ignite go driver starts with creating [Client] using function [Start]
//
//	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
//	defer cancel()
//	client, err := ignite.Start(ctx, ignite.WithAddresses("127.0.0.1:10800", "127.0.0.1:10801")
//	if err != nil {
//		return err
//	}
//	defer func() {
//		_ = client.close(context.Background())
//	}()
//
// This will create a new ignite client to the ignite cluster on localhost. It is possible
// to pass multiple [ClientConfigurationOption] parameters to method Start that specify various
// client options.
//
// # Cache creation
//
// Cache can be created or obtained with help of these methods:
//   - [Client.GetOrCreateCache] gets/creates cache by name.
//   - [Client.CreateCache] creates cache by name.
//   - [Client.GetOrCreateCacheWithConfiguration] gets/creates cache by [CacheConfiguration].
//   - [Client.CreateCacheWithConfiguration] creates cache by [CacheConfiguration].
//
// A list of already created caches can be obtained using [Client.CacheNames]. A cache can be destroyed with [Client.DestroyCache].
//
// # Cache operations
//
// Here is a basic usage scenario of [Cache]:
//
//	ctx := context.Background()
//	cache, err := client.GetOrCreateCacheWithConfiguration(ctx, ignite.CreateCacheConfiguration("test",
//		ignite.WithCacheAtomicityMode(ignite.AtomicAtomicityMode),
//		ignite.WithCacheMode(ignite.ReplicatedCacheMode),
//		ignite.WithReadFromBackup(true)
//	))
//	if err != nil {
//		fmt.Printf("Failed to create cache %s \n", err)
//		return
//	}
//	err = cache.Put(ctx, "test", "test")
//	if err != nil {
//		fmt.Printf("Failed to put value %s \n", err)
//		return
//	}
//	contains, err := cache.ContainsKey(ctx, "test")
//	if err != nil {
//		fmt.Printf("Failed to invoke contains key operation %s \n", err)
//		return
//	}
//	fmt.Printf("Contains key %s? %t\n", "test", contains)
//	>>> Contains key test? true
//
// You can see other operations in [Cache] documentation. Currently only limited types are supported: numerical types, string,
// uuid, bytes slices. Other types and BinaryObject will be added in later releases.
//
// # TTL (ExpiryPolicy) support
//
// You can set ExpiryPolicy to entries by creating special decorator by [Cache.WithExpiryPolicy].
//
//	cache, err := client.GetOrCreateCache(ctx, "cache")
//	if err != nil {
//		fmt.Printf("Failed to create cache %s \n", err)
//		return
//	}
//	cache = cache.WithExpiryPolicy(1*time.Second, DurationZero, DurationZero)
//	err = cache.Put(ctx, "test", "test")
//	if err != nil {
//		fmt.Printf("Failed to put value %s \n", err)
//		return
//	}
//	<-time.After(1200 * time.Millisecond)
//	contains, err := cache.ContainsKey(ctx, "test")
//	if err != nil {
//		fmt.Printf("Failed to invoke contains key operation %s \n", err)
//		return
//	}
//	fmt.Printf("Contains key %s? %t\n", "test", contains)
//	>>> Contains key test? false
//
// Also, you can set ExpiryPolicy for all cache on creation by passing [WithExpiryPolicy] to [CreateCacheConfiguration].
package ignite

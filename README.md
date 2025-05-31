# Ignite Go Client

A Go thin client for [Apache Ignite v2](https://ignite.apache.org/).
This is experimental software mostly taken and refurbished from different sources and python binary helpers for research needs.
The library is distributed "as-is" without any warranty of any kind. How to use it, if use it at all, is solely on your own discretion.

## Requirements

- Go 1.22 or higher

## Installation

```bash
go get github.com/source-c/go-ignit-thin
```

## Usage

### Basic Client Setup

The Apache Ignite Go driver starts with creating a `Client` using the `Start` function:

```go
ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()

client, err := ignite.Start(ctx, ignite.WithAddresses("127.0.0.1:10800", "127.0.0.1:10801"))
if err != nil {
    return err
}
defer func() {
    _ = client.Close(context.Background())
}()
```

This creates a new Ignite client connected to the Ignite cluster on localhost. You can pass multiple `ClientConfigurationOption` parameters to the `Start` method to specify various client options.

### Cache Management

Caches can be created or obtained using these methods:

- `Client.GetOrCreateCache` - Gets or creates a cache by name
- `Client.CreateCache` - Creates a cache by name
- `Client.GetOrCreateCacheWithConfiguration` - Gets or creates a cache using `CacheConfiguration`
- `Client.CreateCacheWithConfiguration` - Creates a cache using `CacheConfiguration`

You can obtain a list of existing caches using `Client.CacheNames`. To destroy a cache, use `Client.DestroyCache`.

### Cache Operations

Here's a basic usage example of the `Cache` interface:

```go
ctx := context.Background()
cache, err := client.GetOrCreateCacheWithConfiguration(ctx, ignite.CreateCacheConfiguration("test",
    ignite.WithCacheAtomicityMode(ignite.AtomicAtomicityMode),
    ignite.WithCacheMode(ignite.ReplicatedCacheMode),
    ignite.WithReadFromBackup(true),
))
if err != nil {
    fmt.Printf("Failed to create cache: %s\n", err)
    return
}

err = cache.Put(ctx, "test", "test")
if err != nil {
    fmt.Printf("Failed to put value: %s\n", err)
    return
}

contains, err := cache.ContainsKey(ctx, "test")
if err != nil {
    fmt.Printf("Failed to invoke contains key operation: %s\n", err)
    return
}
fmt.Printf("Contains key %s? %t\n", "test", contains)
// Output: Contains key test? true
```

For more operations, refer to the `Cache` documentation. Currently, the following types are supported:
- Numerical types
- String
- UUID
- Byte slices

Support for other types and BinaryObject will be added in future releases.

### TTL (ExpiryPolicy) Support

You can set an ExpiryPolicy for entries by creating a special decorator using `Cache.WithExpiryPolicy`:

```go
cache, err := client.GetOrCreateCache(ctx, "cache_name")
if err != nil {
    fmt.Printf("Failed to create cache: %s\n", err)
    return
}

cache = cache.WithExpiryPolicy(1*time.Second, DurationZero, DurationZero)
err = cache.Put(ctx, "test", "test")
if err != nil {
    fmt.Printf("Failed to put value: %s\n", err)
    return
}

<-time.After(1200 * time.Millisecond)
contains, err := cache.ContainsKey(ctx, "test")
if err != nil {
    fmt.Printf("Failed to invoke contains key operation: %s\n", err)
    return
}
fmt.Printf("Contains key %s? %t\n", "test", contains)
// Output: Contains key test? false
```

## License

MIT License
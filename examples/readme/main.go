package main

import (
	"context"
	"fmt"
	ignite "github.com/source-c/go-ignit-thin"
)

func example1() (*ignite.Client, error) {
	// Create a context for the Ignite client
	ctx := context.Background()

	// Create a new Ignite client
	client, err := ignite.Start(ctx, ignite.WithAddresses("127.0.0.1:10800"))
	if err != nil {
		return nil, err
	}

	println("Successfully connected to Ignite cluster")
	return client, nil
}

func example2(client *ignite.Client) {
	ctx := context.Background()
	cache, err := client.GetOrCreateCacheWithConfiguration(
	        ctx, 
	        ignite.CreateCacheConfiguration(
	                "test",
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
}

func main() {
	client, err := example1()
	if err != nil {
		panic(err)
	}
	defer client.Close(context.Background())

	example2(client)
}

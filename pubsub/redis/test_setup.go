package redis

import (
	"context"
	"fmt"
	"log"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redis/go-redis/v9"
)

func RunContainer() (*redis.Client, func() error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "redis",
		Tag:          "7-alpine",
		ExposedPorts: []string{"6379/tcp"},
	}, func(cfg *docker.HostConfig) {
		cfg.AutoRemove = true
		cfg.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("could not start redis container: %s", err)
	}

	addr := fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp"))
	var client *redis.Client
	err = pool.Retry(func() error {
		client = redis.NewClient(&redis.Options{
			Addr: addr,
		})
		return client.Ping(context.Background()).Err()
	})
	if err != nil {
		log.Fatalf("could not connect to redis: %s", err)
	}

	return client, resource.Close
}

package nats

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func RunContainer() (*nats.Conn, func() error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "2.10",
		Cmd: []string{
			"-js", "-sd", "/data", "--auth", "secretnats",
		},
		ExposedPorts: []string{"4222/tcp"},
	}, func(cfg *docker.HostConfig) {
		cfg.AutoRemove = true
		cfg.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("could not start nats container: %s", err)
	}

	natsURL := fmt.Sprintf("nats://secretnats@localhost:%s", resource.GetPort("4222/tcp"))
	var natsConn *nats.Conn
	err = pool.Retry(func() error {
		natsConn, err = nats.Connect(natsURL)
		return err
	})
	if err != nil {
		log.Fatalf("could not connect to nats: %s", err)
	}

	return natsConn, resource.Close
}

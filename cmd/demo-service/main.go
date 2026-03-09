package main

import (
	"flag"
	"log"

	"github.com/Flaasks/distributed-service-registry/internal/config"
)

func main() {
	configPath := flag.String("config", "config/demo-service.example.yaml", "Path to demo service YAML config")
	flag.Parse()

	cfg, err := config.LoadDemoServiceConfig(*configPath)
	if err != nil {
		log.Fatalf("cannot load demo service config: %v", err)
	}

	log.Printf(
		"demo-service bootstrap loaded: id=%s name=%s version=%s endpoint=%s registry_endpoints=%d",
		cfg.Service.ID,
		cfg.Service.Name,
		cfg.Service.Version,
		cfg.Service.Endpoint,
		len(cfg.Registry.Endpoints),
	)
	log.Printf("step 1 scaffold ready: registration and heartbeat flows will be added in next steps")
}

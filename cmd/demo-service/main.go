package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Flaasks/distributed-service-registry/internal/client"
	"github.com/Flaasks/distributed-service-registry/internal/config"
	apiv1 "github.com/Flaasks/distributed-service-registry/pkg/api"
)

func main() {
	configPath := flag.String("config", "config/demo-service.example.yaml", "Path to demo service YAML config")
	flag.Parse()

	cfg, err := config.LoadDemoServiceConfig(*configPath)
	if err != nil {
		log.Fatalf("cannot load demo service config: %v", err)
	}

	log.Printf(
		"demo-service starting: id=%s name=%s version=%s endpoint=%s registry_endpoints=%d",
		cfg.Service.ID,
		cfg.Service.Name,
		cfg.Service.Version,
		cfg.Service.Endpoint,
		len(cfg.Registry.Endpoints),
	)

	rc := client.NewRegistryClient(cfg.Registry.Endpoints, 3*time.Second, 5*time.Second)

	initHealth := parseHealthStatus(cfg.Service.HealthInit)
	record := &apiv1.ServiceRecord{
		ServiceName:  cfg.Service.Name,
		ServiceId:    cfg.Service.ID,
		Endpoint:     cfg.Service.Endpoint,
		Version:      cfg.Service.Version,
		HealthStatus: initHealth,
	}

	// Register with retry loop until first success.
	for {
		if err := rc.Register(record); err != nil {
			log.Printf("register failed (retrying in 3s): %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("registered: service_id=%s name=%s", cfg.Service.ID, cfg.Service.Name)
		break
	}

	heartbeatInterval := time.Duration(cfg.Registry.HeartbeatIntervalSeconds) * time.Second
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

loop:
	for {
		select {
		case <-ticker.C:
			if err := rc.Heartbeat(cfg.Service.Name, cfg.Service.ID, initHealth); err != nil {
				log.Printf("heartbeat failed: %v", err)
			} else {
				log.Printf("heartbeat sent: service_id=%s", cfg.Service.ID)
			}
		case sig := <-signalCh:
			log.Printf("shutdown signal received: %s", sig)
			break loop
		}
	}

	// Graceful deregister on shutdown.
	if err := rc.Deregister(cfg.Service.Name, cfg.Service.ID); err != nil {
		log.Printf("deregister failed: %v", err)
	} else {
		log.Printf("deregistered: service_id=%s", cfg.Service.ID)
	}
}

// parseHealthStatus converts the YAML string value to the proto enum.
// Defaults to SERVING for unrecognised or empty values.
func parseHealthStatus(s string) apiv1.HealthStatus {
	switch s {
	case "HEALTH_STATUS_SERVING":
		return apiv1.HealthStatus_HEALTH_STATUS_SERVING
	case "HEALTH_STATUS_NOT_SERVING":
		return apiv1.HealthStatus_HEALTH_STATUS_NOT_SERVING
	case "HEALTH_STATUS_DEGRADED":
		return apiv1.HealthStatus_HEALTH_STATUS_DEGRADED
	default:
		return apiv1.HealthStatus_HEALTH_STATUS_SERVING
	}
}

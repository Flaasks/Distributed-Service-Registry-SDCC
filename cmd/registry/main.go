package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Flaasks/distributed-service-registry/internal/config"
	"github.com/Flaasks/distributed-service-registry/internal/gossip"
	"github.com/Flaasks/distributed-service-registry/internal/registry"
	"github.com/Flaasks/distributed-service-registry/internal/storage"
	apiv1 "github.com/Flaasks/distributed-service-registry/pkg/api"
)

func main() {
	configPath := flag.String("config", "config/registry.example.yaml", "Path to registry node YAML config")
	flag.Parse()

	cfg, err := config.LoadRegistryConfig(*configPath)
	if err != nil {
		log.Fatalf("cannot load registry config: %v", err)
	}

	listener, err := net.Listen("tcp", cfg.Node.ListenAddress)
	if err != nil {
		log.Fatalf("cannot listen on %s: %v", cfg.Node.ListenAddress, err)
	}

	store := storage.NewServiceStore()
	peerStore := storage.NewPeerStore()
	peerStore.UpsertSelf(cfg.Node.ID, cfg.Node.AdvertiseAddress, time.Now().Unix())

	serviceServer := registry.NewServiceRegistryServer(
		store,
		cfg.Node.ID,
		time.Duration(cfg.Service.HeartbeatTTLSeconds)*time.Second,
	)
	peerServer := registry.NewRegistryPeerServer(store, peerStore, cfg.Node.ID, cfg.Node.AdvertiseAddress)
	gossipRuntime := gossip.NewRuntime(cfg, store, peerStore)

	grpcServer := grpc.NewServer()
	apiv1.RegisterServiceRegistryServer(grpcServer, serviceServer)
	apiv1.RegisterRegistryPeerServer(grpcServer, peerServer)
	gossipRuntime.Start()

	log.Printf(
		"registry node starting: node_id=%s listen=%s advertise=%s seed_peers=%d",
		cfg.Node.ID,
		cfg.Node.ListenAddress,
		cfg.Node.AdvertiseAddress,
		len(cfg.Cluster.SeedPeers),
	)

	serveErr := make(chan error, 1)
	go func() {
		if serveErrValue := grpcServer.Serve(listener); serveErrValue != nil {
			serveErr <- serveErrValue
		}
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		log.Printf("grpc server failed: %v", err)
		gossipRuntime.Stop()
		os.Exit(1)
	case sig := <-signalCh:
		log.Printf("shutdown signal received: %s", sig.String())
		gossipRuntime.Stop()
		gossipRuntime.GracefulLeave()
		grpcServer.GracefulStop()
		log.Printf("registry node stopped")
	}
}

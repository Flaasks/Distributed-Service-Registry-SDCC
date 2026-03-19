package integration

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Flaasks/distributed-service-registry/internal/config"
	"github.com/Flaasks/distributed-service-registry/internal/gossip"
	"github.com/Flaasks/distributed-service-registry/internal/registry"
	"github.com/Flaasks/distributed-service-registry/internal/storage"
	apiv1 "github.com/Flaasks/distributed-service-registry/pkg/api"
)

type testNode struct {
	id          string
	address     string
	service     *storage.ServiceStore
	peers       *storage.PeerStore
	gossip      *gossip.Runtime
	grpcServer  *grpc.Server
	listener    net.Listener
	stopOnce    sync.Once
	serveErrCh  chan error
	serveStopCh chan struct{}
}

func startTestNode(t *testing.T, id string, seedPeers []string) *testNode {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	address := listener.Addr().String()
	cfg := &config.RegistryConfig{
		Node: config.RegistryNodeConfig{
			ID:               id,
			ListenAddress:    address,
			AdvertiseAddress: address,
		},
		Cluster: config.RegistryClusterConfig{
			SeedPeers:                append([]string(nil), seedPeers...),
			GossipIntervalSeconds:    1,
			ReconcileIntervalSeconds: 1,
			PeerTimeoutSeconds:       2,
			MaxGossipFanout:          2,
		},
		Service: config.RegistryServiceConfig{HeartbeatTTLSeconds: 6},
	}

	serviceStore := storage.NewServiceStore()
	peerStore := storage.NewPeerStore()
	peerStore.UpsertSelf(id, address, time.Now().Unix())

	grpcServer := grpc.NewServer()
	serviceServer := registry.NewServiceRegistryServer(serviceStore, id, 6*time.Second)
	peerServer := registry.NewRegistryPeerServer(serviceStore, peerStore, id, address)
	apiv1.RegisterServiceRegistryServer(grpcServer, serviceServer)
	apiv1.RegisterRegistryPeerServer(grpcServer, peerServer)

	node := &testNode{
		id:          id,
		address:     address,
		service:     serviceStore,
		peers:       peerStore,
		grpcServer:  grpcServer,
		listener:    listener,
		serveErrCh:  make(chan error, 1),
		serveStopCh: make(chan struct{}),
	}

	go func() {
		if serveErr := grpcServer.Serve(listener); serveErr != nil {
			select {
			case <-node.serveStopCh:
				return
			default:
			}
			node.serveErrCh <- serveErr
		}
	}()

	node.gossip = gossip.NewRuntime(cfg, serviceStore, peerStore)
	node.gossip.Start()

	return node
}

func (n *testNode) Stop() {
	n.stopOnce.Do(func() {
		close(n.serveStopCh)
		if n.gossip != nil {
			n.gossip.Stop()
		}
		if n.grpcServer != nil {
			n.grpcServer.GracefulStop()
		}
		if n.listener != nil {
			_ = n.listener.Close()
		}
	})
}

func TestMultiNodeGossipConvergenceAndPeerPruning(t *testing.T) {
	nodeA := startTestNode(t, "node-a", nil)
	defer nodeA.Stop()

	nodeB := startTestNode(t, "node-b", []string{nodeA.address})
	defer nodeB.Stop()

	nodeC := startTestNode(t, "node-c", []string{nodeA.address})
	defer nodeC.Stop()

	waitFor(t, 8*time.Second, "node-a sees both other peers", func() bool {
		peers := nodeA.peers.List()
		return hasPeer(peers, "node-b") && hasPeer(peers, "node-c")
	})

	err := registerService(nodeA.address, &apiv1.ServiceRecord{
		ServiceName:  "users",
		ServiceId:    "users-1",
		Endpoint:     "users-1:8080",
		Version:      "v1.0.0",
		HealthStatus: apiv1.HealthStatus_HEALTH_STATUS_SERVING,
	})
	if err != nil {
		t.Fatalf("register service on node-a failed: %v", err)
	}

	waitFor(t, 8*time.Second, "service converges to node-b", func() bool {
		return hasService(nodeB.service.List(), "users", "users-1")
	})
	waitFor(t, 8*time.Second, "service converges to node-c", func() bool {
		return hasService(nodeC.service.List(), "users", "users-1")
	})

	nodeC.Stop()

	waitFor(t, 10*time.Second, "node-c pruned as stale by node-a", func() bool {
		peers := nodeA.peers.List()
		return hasPeer(peers, "node-b") && !hasPeer(peers, "node-c")
	})
}

func registerService(address string, record *apiv1.ServiceRecord) error {
	return withServiceClient(address, func(client apiv1.ServiceRegistryClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		resp, err := client.RegisterService(ctx, &apiv1.RegisterServiceRequest{Record: record})
		if err != nil {
			return err
		}
		if !resp.GetAccepted() {
			return fmt.Errorf("register rejected: %s", resp.GetMessage())
		}
		return nil
	})
}

func withServiceClient(address string, fn func(client apiv1.ServiceRegistryClient) error) error {
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 3*time.Second)
	conn, err := grpc.DialContext(dialCtx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	dialCancel()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	return fn(apiv1.NewServiceRegistryClient(conn))
}

func waitFor(t *testing.T, timeout time.Duration, message string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for condition: %s", message)
}

func hasPeer(peers []*apiv1.NodeInfo, nodeID string) bool {
	for _, peer := range peers {
		if peer.GetNodeId() == nodeID {
			return true
		}
	}
	return false
}

func hasService(records []*apiv1.ServiceRecord, serviceName, serviceID string) bool {
	for _, record := range records {
		if record.GetServiceName() == serviceName && record.GetServiceId() == serviceID {
			return true
		}
	}
	return false
}

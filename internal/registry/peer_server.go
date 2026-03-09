package registry

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Flaasks/distributed-service-registry/internal/storage"
	apiv1 "github.com/Flaasks/distributed-service-registry/pkg/api"
)

type RegistryPeerServer struct {
	apiv1.UnimplementedRegistryPeerServer

	store            *storage.ServiceStore
	nodeID           string
	advertiseAddress string
	now              func() time.Time
}

func NewRegistryPeerServer(store *storage.ServiceStore, nodeID string, advertiseAddress string) *RegistryPeerServer {
	return &RegistryPeerServer{
		store:            store,
		nodeID:           strings.TrimSpace(nodeID),
		advertiseAddress: strings.TrimSpace(advertiseAddress),
		now:              time.Now,
	}
}

func (s *RegistryPeerServer) JoinCluster(_ context.Context, req *apiv1.JoinClusterRequest) (*apiv1.JoinClusterResponse, error) {
	if req == nil || req.GetNode() == nil {
		return nil, status.Error(codes.InvalidArgument, "node is required")
	}

	response := &apiv1.JoinClusterResponse{
		Records: s.store.List(),
		Peers: []*apiv1.NodeInfo{
			{
				NodeId:        s.nodeID,
				GrpcAddress:   s.advertiseAddress,
				UpdatedAtUnix: s.now().Unix(),
			},
		},
	}
	return response, nil
}

func (s *RegistryPeerServer) GossipSync(_ context.Context, _ *apiv1.GossipSyncRequest) (*apiv1.GossipSyncResponse, error) {
	return &apiv1.GossipSyncResponse{
		Accepted:       true,
		ReceivedAtUnix: s.now().Unix(),
	}, nil
}

func (s *RegistryPeerServer) PullState(_ context.Context, _ *apiv1.PullStateRequest) (*apiv1.PullStateResponse, error) {
	return &apiv1.PullStateResponse{
		Records: s.store.List(),
		Peers: []*apiv1.NodeInfo{
			{
				NodeId:        s.nodeID,
				GrpcAddress:   s.advertiseAddress,
				UpdatedAtUnix: s.now().Unix(),
			},
		},
	}, nil
}

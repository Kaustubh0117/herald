package rpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Kaustubh0117/herald/gen/heraldv1"
	"github.com/Kaustubh0117/herald/internal/gateway"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

// MatcherServer exposes a Matcher over gRPC.
type MatcherServer struct {
	pb.UnimplementedMatcherServer
	M *server.Matcher
}

func (s *MatcherServer) Publish(_ context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	for _, it := range req.Items {
		if err := s.M.Publish(p.ObjectID(it.ObjectId), p.Version(it.Version), p.ClientID(it.SourceClient)); err != nil {
			return nil, err
		}
	}
	return &pb.PublishResponse{}, nil
}

func (s *MatcherServer) AddRegistration(_ context.Context, req *pb.RegistrationRequest) (*pb.RegistrationResponse, error) {
	return &pb.RegistrationResponse{}, s.M.AddRegistration(p.ObjectID(req.ObjectId), p.ClientID(req.ClientId))
}

func (s *MatcherServer) RemoveRegistration(_ context.Context, req *pb.RegistrationRequest) (*pb.RegistrationResponse, error) {
	return &pb.RegistrationResponse{}, s.M.RemoveRegistration(p.ObjectID(req.ObjectId), p.ClientID(req.ClientId))
}

func (s *MatcherServer) LatestVersion(_ context.Context, req *pb.LatestVersionRequest) (*pb.LatestVersionResponse, error) {
	v, known, err := s.M.LatestVersion(p.ObjectID(req.ObjectId))
	return &pb.LatestVersionResponse{Version: int64(v), Known: known}, err
}

func (s *MatcherServer) Epoch(context.Context, *pb.EpochRequest) (*pb.EpochResponse, error) {
	e, err := s.M.Epoch()
	return &pb.EpochResponse{Epoch: e}, err
}

// RegistrarServer exposes a Registrar over gRPC.
type RegistrarServer struct {
	pb.UnimplementedRegistrarServer
	R *server.Registrar
}

func (s *RegistrarServer) Handle(_ context.Context, m *pb.ClientMessage) (*pb.ServerMessage, error) {
	resp, err := s.R.Handle(clientMessageFromPB(m))
	if err != nil {
		return nil, err
	}
	return serverMessageToPB(resp), nil
}

func (s *RegistrarServer) Deliver(_ context.Context, req *pb.DeliverRequest) (*pb.DeliverResponse, error) {
	for _, it := range req.Items {
		err := s.R.Deliver(p.ClientID(it.ClientId), notifFromPB(it.Notification))
		if errors.Is(err, server.ErrNotRegistered) {
			return nil, status.Error(codes.NotFound, err.Error()) // the Matcher drops the registration
		}
		if err != nil {
			return nil, err
		}
	}
	return &pb.DeliverResponse{}, nil
}

// GatewayServer exposes a Gateway's fast path over gRPC.
type GatewayServer struct {
	pb.UnimplementedGatewayServer
	G *gateway.Gateway
}

func (s *GatewayServer) Push(_ context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	s.G.Push(p.ClientID(req.ClientId), notifsFromPB(req.Notifications))
	return &pb.PushResponse{}, nil
}

package server

import (
	"context"

	api "github.com/burmudar/prolog/api/v1"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(context context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(context context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {

		// Receive the request over the stream
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// Action the request by sending it to the Produce endpoint and receive a result with a record back
		resp, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		// Respond to the client on the stream by sending the record
		if err = stream.Send(resp); err != nil {
			return err
		}

	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			resp, err := s.Consume(stream.Context(), req)

			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err = stream.Send(resp); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

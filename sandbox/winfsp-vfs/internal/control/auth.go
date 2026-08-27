package control

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func authorize(ctx context.Context, expected string) error {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return status.Error(codes.Unauthenticated, "missing control authorization")
	}
	want := "Bearer " + strings.TrimSpace(expected)
	got := values[0]
	if len(got) != len(want) || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid control authorization")
	}
	return nil
}

func unaryAuth(expected string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorize(ctx, expected); err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
}

func streamAuth(expected string) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorize(stream.Context(), expected); err != nil {
			return err
		}
		return handler(server, stream)
	}
}

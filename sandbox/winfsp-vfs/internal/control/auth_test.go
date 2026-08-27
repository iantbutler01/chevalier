package control

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthorize(t *testing.T) {
	valid := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	if err := authorize(valid, "secret"); err != nil {
		t.Fatalf("valid authorization rejected: %v", err)
	}
	for _, ctx := range []context.Context{
		context.Background(),
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong")),
	} {
		if code := status.Code(authorize(ctx, "secret")); code != codes.Unauthenticated {
			t.Fatalf("authorization failure returned %s", code)
		}
	}
}

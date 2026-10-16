// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/operationjournal"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type lifecycleDriver interface {
	Run(context.Context, *ateletpb.RunRequest) (*ateletpb.RunResponse, error)
	Restore(context.Context, *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error)
	Checkpoint(context.Context, *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error)
	Terminate(context.Context, *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error)
}

type failTerminalJournal struct{ *operationjournal.Journal }

func (j *failTerminalJournal) Complete(string, string, []byte) error {
	return errors.New("injected interruption after effects before terminal receipt")
}

type receiptLifecycleDriver struct {
	service                *AteomHerder
	journal                *operationjournal.Journal
	failTerminationReceipt bool
	client                 ateletpb.AteomHerderClient
	operations             []*ateletpb.ExecuteOperationRequest
}

func receiptPeerContext(t *testing.T, ctx context.Context, account string) context.Context {
	t.Helper()
	cert := &x509.Certificate{}
	id := &substratex509.PodIdentity{Namespace: "system", ServiceAccountName: "ateapi", ServiceAccountUID: account, PodName: "ateapi-1", PodUID: "api-pod-1", NodeName: "node-1", NodeUID: "node-uid-1"}
	if err := substratex509.AddPodIdentityToCertificate(id, cert); err != nil {
		t.Fatal(err)
	}
	cert.Extensions = cert.ExtraExtensions
	return peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}}})
}

func newReceiptLifecycleDriver(t *testing.T, s *AteomHerder) *receiptLifecycleDriver {
	t.Helper()
	journal := &operationjournal.Journal{Root: t.TempDir(), Limit: 32}
	s.operationJournal = journal
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		return next(receiptPeerContext(t, ctx, "api-account-uid"), req)
	}))
	ateletpb.RegisterAteomHerderServer(srv, s)
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); lis.Close(); <-done })
	conn, err := grpc.NewClient("passthrough:///native-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &receiptLifecycleDriver{client: ateletpb.NewAteomHerderClient(conn), service: s, journal: journal}
}

func (d *receiptLifecycleDriver) execute(ctx context.Context, req *ateletpb.ExecuteOperationRequest) error {
	req.OperationId = fmt.Sprintf("persisted-operation-%d", len(d.operations))
	req.WorkerUid = "worker-uid-1"
	req.WorkerPodUid = "ateom-uid-1"
	d.operations = append(d.operations, proto.CloneOf(req))
	result, err := d.client.ExecuteOperation(ctx, req)
	if err != nil {
		return err
	}
	if result.GetState() != ateletpb.OperationState_OPERATION_STATE_SUCCEEDED {
		return fmt.Errorf("unexpected receipt: %v", result)
	}
	// Discard the original response and obtain authoritative terminal evidence.
	result, err = d.client.GetOperation(ctx, &ateletpb.GetOperationRequest{Operation: req})
	if err != nil {
		return err
	}
	if result.GetState() != ateletpb.OperationState_OPERATION_STATE_SUCCEEDED {
		return fmt.Errorf("lookup: %v", result)
	}
	return nil
}
func (d *receiptLifecycleDriver) Run(ctx context.Context, r *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	err := d.execute(ctx, &ateletpb.ExecuteOperationRequest{Request: &ateletpb.ExecuteOperationRequest_Run{Run: r}})
	return &ateletpb.RunResponse{}, err
}
func (d *receiptLifecycleDriver) Restore(ctx context.Context, r *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	err := d.execute(ctx, &ateletpb.ExecuteOperationRequest{Request: &ateletpb.ExecuteOperationRequest_Restore{Restore: r}})
	return &ateletpb.RestoreResponse{}, err
}
func (d *receiptLifecycleDriver) Checkpoint(ctx context.Context, r *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	err := d.execute(ctx, &ateletpb.ExecuteOperationRequest{Request: &ateletpb.ExecuteOperationRequest_Checkpoint{Checkpoint: r}})
	return &ateletpb.CheckpointResponse{}, err
}
func (d *receiptLifecycleDriver) Terminate(ctx context.Context, r *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	if d.failTerminationReceipt {
		d.service.operationJournal = &failTerminalJournal{Journal: d.journal}
	}

	err := d.execute(ctx, &ateletpb.ExecuteOperationRequest{Request: &ateletpb.ExecuteOperationRequest_Terminate{Terminate: r}})
	if d.failTerminationReceipt && status.Code(err) == codes.Unavailable {
		return &ateletpb.TerminateResponse{}, nil
	}
	return &ateletpb.TerminateResponse{}, err
}

func (d *receiptLifecycleDriver) checkRecovery(t *testing.T, previous *AteomHerder) {
	t.Helper()
	// New service instance, real on-disk receipts, actor directories already
	// deleted. No sandbox/image/object-store clients exist to repeat effects.
	s := &AteomHerder{operationJournal: &operationjournal.Journal{Root: d.journal.Root, Limit: 32}}
	ctx := receiptPeerContext(t, t.Context(), "api-account-uid")
	for i, req := range d.operations {
		expected := ateletpb.OperationState_OPERATION_STATE_SUCCEEDED
		if d.failTerminationReceipt && i == len(d.operations)-1 {
			expected = ateletpb.OperationState_OPERATION_STATE_UNRESOLVED
		}
		result, err := s.GetOperation(ctx, &ateletpb.GetOperationRequest{Operation: req})
		if err != nil || result.GetState() != expected {
			t.Fatalf("restart lookup: %v %v", result, err)
		}
		result, err = s.ExecuteOperation(ctx, req)
		if err != nil || result.GetState() != expected {
			t.Fatalf("restart replay: %v %v", result, err)
		}
		changed := proto.CloneOf(req)
		changed.WorkerUid = "replaced-worker"
		if _, err = s.GetOperation(ctx, &ateletpb.GetOperationRequest{Operation: changed}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("changed identity: %v", err)
		}
		if _, err = s.GetOperation(receiptPeerContext(t, t.Context(), "other-account"), &ateletpb.GetOperationRequest{Operation: req}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("other principal: %v", err)
		}
	}
	if _, err := s.GetOperation(t.Context(), &ateletpb.GetOperationRequest{Operation: d.operations[0]}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated lookup: %v", err)
	}
}

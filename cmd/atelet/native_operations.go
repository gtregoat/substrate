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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/operationjournal"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func operationHasUnknown(m protoreflect.Message) bool {
	if len(m.GetUnknown()) != 0 {
		return true
	}
	unknown := false
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.IsMap() && f.MapValue().Kind() == protoreflect.MessageKind {
			v.Map().Range(func(_ protoreflect.MapKey, e protoreflect.Value) bool {
				unknown = operationHasUnknown(e.Message())
				return !unknown
			})
		} else if f.IsList() && f.Kind() == protoreflect.MessageKind {
			for i := 0; i < v.List().Len(); i++ {
				if operationHasUnknown(v.List().Get(i).Message()) {
					unknown = true
					break
				}
			}
		} else if f.Kind() == protoreflect.MessageKind && !f.IsList() && !f.IsMap() {
			unknown = operationHasUnknown(v.Message())
		}
		return !unknown
	})
	return unknown
}

func validateOperation(req *ateletpb.ExecuteOperationRequest) error {
	if req == nil || len(req.GetOperationId()) < 16 || len(req.GetOperationId()) > 128 || req.GetWorkerUid() == "" || req.GetWorkerPodUid() == "" || operationHasUnknown(req.ProtoReflect()) {
		return status.Error(codes.InvalidArgument, "complete known operation identity required")
	}
	var err error
	var target string
	switch r := req.GetRequest().(type) {
	case *ateletpb.ExecuteOperationRequest_Run:
		if r.Run == nil {
			return status.Error(codes.InvalidArgument, "missing run")
		}
		err = validateRunRequest(r.Run)
		target = r.Run.GetTargetAteomUid()
	case *ateletpb.ExecuteOperationRequest_Restore:
		if r.Restore == nil {
			return status.Error(codes.InvalidArgument, "missing restore")
		}
		err = validateRestoreRequest(r.Restore)
		target = r.Restore.GetTargetAteomUid()
	case *ateletpb.ExecuteOperationRequest_Checkpoint:
		if r.Checkpoint == nil {
			return status.Error(codes.InvalidArgument, "missing checkpoint")
		}
		err = validateCheckpointRequest(r.Checkpoint)
		target = r.Checkpoint.GetTargetAteomUid()
	case *ateletpb.ExecuteOperationRequest_Upload:
		if r.Upload == nil {
			return status.Error(codes.InvalidArgument, "missing upload")
		}
		err = validateUploadPausedCheckpointRequest(r.Upload)
		target = req.GetWorkerPodUid()
	case *ateletpb.ExecuteOperationRequest_Terminate:
		if r.Terminate == nil {
			return status.Error(codes.InvalidArgument, "missing terminate")
		}
		err = validateTerminateRequest(r.Terminate)
		target = r.Terminate.GetTargetAteomUid()
	default:
		return status.Error(codes.InvalidArgument, "missing operation request")
	}
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if target != req.GetWorkerPodUid() {
		return status.Error(codes.FailedPrecondition, "operation target incarnation mismatch")
	}
	return nil
}

func operationPrincipal(ctx context.Context) (string, error) {
	id, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return "", err
	}
	if id.Namespace == "" || id.ServiceAccountUID == "" {
		return "", status.Error(codes.PermissionDenied, "qualified service account identity required")
	}
	// Pod restarts keep the service-account principal; recreation of the account
	// does not inherit access to its predecessor's operation receipts.
	return id.Namespace + "/" + id.ServiceAccountUID, nil
}

func operationResult(req *ateletpb.ExecuteOperationRequest, b []byte, state operationjournal.State) *ateletpb.OperationResult {
	h := sha256.Sum256(b)
	result := &ateletpb.OperationResult{OperationId: req.GetOperationId(), RequestSha256: hex.EncodeToString(h[:]), State: ateletpb.OperationState_OPERATION_STATE_UNRESOLVED}
	switch state {
	case operationjournal.Missing:
		result.State = ateletpb.OperationState_OPERATION_STATE_MISSING
	case operationjournal.Succeeded:
		result.State = ateletpb.OperationState_OPERATION_STATE_SUCCEEDED
	}
	return result
}

func operationStorageError(err error) error {
	if errors.Is(err, operationjournal.ErrConflict) {
		return status.Error(codes.FailedPrecondition, "operation identity or stored receipt incompatible")
	}
	if errors.Is(err, operationjournal.ErrFull) {
		return status.Error(codes.ResourceExhausted, "native operation receipt capacity exhausted")
	}
	return status.Error(codes.Unavailable, "native operation receipt storage unavailable")
}

func (s *AteomHerder) GetOperation(ctx context.Context, req *ateletpb.GetOperationRequest) (*ateletpb.OperationResult, error) {
	principal, err := operationPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "exact operation required")
	}
	if err = validateOperation(req.GetOperation()); err != nil {
		return nil, err
	}
	if s.operationJournal == nil {
		return nil, status.Error(codes.Unimplemented, "native operation journal unavailable")
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(req.GetOperation())
	if err != nil {
		return nil, err
	}
	state, err := s.operationJournal.Lookup(req.Operation.OperationId, principal, b)
	if err != nil {
		return nil, operationStorageError(err)
	}
	return operationResult(req.Operation, b, state), nil
}

func (s *AteomHerder) ExecuteOperation(ctx context.Context, req *ateletpb.ExecuteOperationRequest) (*ateletpb.OperationResult, error) {
	principal, err := operationPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if err = validateOperation(req); err != nil {
		return nil, err
	}
	if s.operationJournal == nil {
		return nil, status.Error(codes.Unimplemented, "native operation journal unavailable")
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(req)
	if err != nil {
		return nil, err
	}
	state, fresh, err := s.operationJournal.Accept(req.OperationId, principal, b)
	if err != nil {
		return nil, operationStorageError(err)
	}
	if !fresh {
		return operationResult(req, b, state), nil
	}
	// Acceptance is durable before any production operation starts. An error
	// never removes that acceptance or authorizes another dispatch.
	switch r := req.GetRequest().(type) {
	case *ateletpb.ExecuteOperationRequest_Run:
		_, err = s.Run(ctx, r.Run)
	case *ateletpb.ExecuteOperationRequest_Restore:
		_, err = s.Restore(ctx, r.Restore)
	case *ateletpb.ExecuteOperationRequest_Checkpoint:
		_, err = s.Checkpoint(ctx, r.Checkpoint)
		if err == nil && r.Checkpoint.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL {
			err = pruneLocalCheckpoints(ctx, r.Checkpoint.GetActorUid())
		}
	case *ateletpb.ExecuteOperationRequest_Upload:
		// Legacy upload can infer success from a manifest alone. This contract
		// requires this invocation to perform the upload from its actual source.
		_, err = os.Stat(filepath.Join(ateompath.LocalSnapshotDir(r.Upload.GetActorUid(), r.Upload.GetLocalSnapshotName()), sandboxManifestName))
		if err == nil {
			_, err = s.UploadPausedCheckpoint(ctx, r.Upload)
		}
		if err == nil {
			err = pruneLocalCheckpoints(ctx, r.Upload.GetActorUid())
		}
	case *ateletpb.ExecuteOperationRequest_Terminate:
		_, err = s.Terminate(ctx, r.Terminate)
	default:
		err = fmt.Errorf("unsupported accepted operation")
	}
	if err != nil {
		return nil, err
	}
	if err = s.operationJournal.Complete(req.OperationId, principal, b); err != nil {
		return nil, operationStorageError(err)
	}
	return operationResult(req, b, operationjournal.Succeeded), nil
}

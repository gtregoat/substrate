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

package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequireSettledNativeOperation(t *testing.T) {
	for _, s := range []*ateapipb.ActorStatus{
		{PendingNativeOperation: &ateapipb.NativeOperationOwnership{Operation: "checkpoint-local"}},
		{InProgressLocalSnapshotName: "legacy-unlocated"},
		{InProgressSnapshotUri: "gs://legacy/unknown"},
	} {
		if err := requireSettledNativeOperation(&ateapipb.Actor{Status: s}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("unresolved origin allowed recovery: %v", err)
		}
	}
	if err := requireSettledNativeOperation(&ateapipb.Actor{Status: &ateapipb.ActorStatus{
		ExternalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://retained/external"},
		LocalSnapshot:    &ateapipb.LocalSnapshot{SnapshotName: "settled-local", NodeVmsWithLocalSnapshots: []string{"node"}},
	}}); err != nil {
		t.Fatalf("settled history prevented legitimate recovery: %v", err)
	}
}

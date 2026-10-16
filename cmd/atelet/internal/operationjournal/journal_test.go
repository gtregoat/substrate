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

package operationjournal

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRestartAndLostResponse(t *testing.T) {
	root := t.TempDir()
	j := &Journal{Root: root, Limit: 4}
	request := []byte("exact actor and worker incarnation, request")
	state, fresh, err := j.Accept("token", "service-account-uid", request)
	if err != nil || !fresh || state != Unresolved {
		t.Fatalf("accept: %s %v %v", state, fresh, err)
	}
	// Restart after durable acceptance, before effects: no caller may replay.
	j = &Journal{Root: root, Limit: 4}
	state, fresh, err = j.Accept("token", "service-account-uid", request)
	if err != nil || fresh || state != Unresolved {
		t.Fatalf("restart: %s %v %v", state, fresh, err)
	}
	if err = j.Complete("token", "service-account-uid", request); err != nil {
		t.Fatal(err)
	}
	// Completion survives a lost response, another restart and sandbox deletion.
	sandbox := filepath.Join(root, "sandbox")
	if err = os.Mkdir(sandbox, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(sandbox); err != nil {
		t.Fatal(err)
	}
	j = &Journal{Root: root, Limit: 4}
	state, fresh, err = j.Accept("token", "service-account-uid", request)
	if err != nil || fresh || state != Succeeded {
		t.Fatalf("lost ACK: %s %v %v", state, fresh, err)
	}
	state, err = j.Lookup("missing", "service-account-uid", request)
	if err != nil || state != Missing {
		t.Fatalf("missing: %s %v", state, err)
	}
}

func TestConflictAndCapacityNeverForget(t *testing.T) {
	j := &Journal{Root: t.TempDir(), Limit: 1}
	req := []byte("request")
	if _, _, err := j.Accept("token", "principal", req); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		principal string
		request   []byte
	}{{"other", req}, {"principal", []byte("changed actor/worker/parameters")}} {
		if _, _, err := j.Accept("token", tc.principal, tc.request); !errors.Is(err, ErrConflict) {
			t.Fatalf("accept conflict: %v", err)
		}
		if _, err := j.Lookup("token", tc.principal, tc.request); !errors.Is(err, ErrConflict) {
			t.Fatalf("lookup conflict: %v", err)
		}
	}
	if err := j.Complete("token", "principal", req); err != nil {
		t.Fatal(err)
	}
	if _, _, err := j.Accept("new", "principal", req); !errors.Is(err, ErrFull) {
		t.Fatalf("capacity: %v", err)
	}
	state, fresh, err := j.Accept("token", "principal", req)
	if err != nil || fresh || state != Succeeded {
		t.Fatalf("retention: %s %v %v", state, fresh, err)
	}
}

func TestConcurrentAcceptance(t *testing.T) {
	root := t.TempDir()
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for i := 0; i < 32; i++ {
		wg.Go(func() {
			// Distinct instances use filesystem locking rather than process-local state.
			j := &Journal{Root: root, Limit: 4}
			_, fresh, err := j.Accept("same-token", "principal", []byte("same request"))
			if err != nil {
				t.Error(err)
			}
			if fresh {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("executors: %d", accepted.Load())
	}
}

func TestCorruptReceiptAndFailedStorageRefuse(t *testing.T) {
	root := t.TempDir()
	j := &Journal{Root: root, Limit: 4}
	if err := os.WriteFile(j.path("token"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, fresh, err := j.Accept("token", "principal", []byte("request")); err == nil || fresh {
		t.Fatalf("corruption accepted: %v %v", fresh, err)
	}
	j.Root = filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(j.Root, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, fresh, err := j.Accept("new", "principal", []byte("request")); err == nil || fresh {
		t.Fatalf("failed storage accepted: %v %v", fresh, err)
	}
}

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

// Package operationjournal retains native acceptance independently of actor data.
package operationjournal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

var (
	ErrConflict = errors.New("operation identity conflict")
	ErrFull     = errors.New("operation receipt capacity exhausted")
)

type State string

const (
	Missing    State = "missing"
	Unresolved State = "unresolved"
	Succeeded  State = "succeeded"
)

type Receipt struct {
	Version   int    `json:"version"`
	Token     string `json:"token"`
	Principal string `json:"principal"`
	Request   []byte `json:"request"`
	State     State  `json:"state"`
}

// Journal bounds storage by refusing new acceptance at capacity. It never
// evicts a receipt: an old token can still be retried by a disconnected caller.
// Filesystem locking also excludes overlapping atelet processes on this node.
type Journal struct {
	Root  string
	Limit int
}

func (j *Journal) locked(fn func() error) error {
	if j.Limit < 1 {
		return ErrFull
	}
	if err := os.MkdirAll(j.Root, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(j.Root, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

func (j *Journal) path(token string) string {
	h := sha256.Sum256([]byte(token))
	return filepath.Join(j.Root, hex.EncodeToString(h[:])+".json")
}

func (j *Journal) read(token, principal string, request []byte) (*Receipt, error) {
	b, err := os.ReadFile(j.path(token))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r Receipt
	if err = json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Version != 1 || r.Token != token || r.Principal != principal || string(r.Request) != string(request) || (r.State != Unresolved && r.State != Succeeded) {
		return nil, ErrConflict
	}
	return &r, nil
}

func syncDir(root string) error {
	d, err := os.Open(root)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (j *Journal) write(r *Receipt) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(j.Root, ".receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), j.path(r.Token)); err != nil {
		return err
	}
	return syncDir(j.Root)
}

// Accept returns fresh only after durable acceptance. Only that caller may
// execute effects. Reopening a journal never grants execution for an old token.
func (j *Journal) Accept(token, principal string, request []byte) (state State, fresh bool, err error) {
	if token == "" || principal == "" || len(request) == 0 || len(request) > 1<<20 {
		return "", false, ErrConflict
	}
	err = j.locked(func() error {
		r, e := j.read(token, principal, request)
		if e != nil {
			return e
		}
		if r != nil {
			state = r.State
			return nil
		}
		entries, e := os.ReadDir(j.Root)
		if e != nil {
			return e
		}
		// Count temporary files too: storage failure must not permit unbounded use.
		if len(entries)-1 >= j.Limit {
			return ErrFull
		}
		r = &Receipt{Version: 1, Token: token, Principal: principal, Request: request, State: Unresolved}
		if e = j.write(r); e != nil {
			return e
		}
		state = Unresolved
		fresh = true
		return nil
	})
	return
}

func (j *Journal) Lookup(token, principal string, request []byte) (state State, err error) {
	err = j.locked(func() error {
		r, e := j.read(token, principal, request)
		if e != nil {
			return e
		}
		state = Missing
		if r != nil {
			state = r.State
		}
		return nil
	})
	return
}

// Complete is called only after the production operation and its required local
// disposition succeed. Failure leaves acceptance owned and prohibits replay.
func (j *Journal) Complete(token, principal string, request []byte) error {
	return j.locked(func() error {
		r, e := j.read(token, principal, request)
		if e != nil {
			return e
		}
		if r == nil {
			return fmt.Errorf("completion without acceptance: %w", ErrConflict)
		}
		r.State = Succeeded
		return j.write(r)
	})
}

/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package kubernetes provides a distributed lock implementation backed by
// Kubernetes coordination.k8s.io/v1 Lease objects. Each ResourceID maps to a
// single Lease resource. Acquisition creates or takes over a Lease; release
// deletes it. Lock expiry is enforced by comparing the Lease renewTime plus
// leaseDurationSeconds against the current time.
//
// Caveats to be aware of before using this component:
//
//  1. The pod running Dapr must have RBAC permissions to get/create/update/delete
//     Leases in the configured namespace.
//  2. ResourceIDs are hashed (sha256, truncated) to produce valid DNS-1123
//     Lease names. The original ResourceID is preserved as an annotation on
//     the Lease for operator visibility but is not usable for lookups.
//  3. Each TryLock/Unlock performs a round-trip to the kube-apiserver. This
//     component is intended for coordination (leader election, singleton
//     workloads) rather than high-frequency locking.
//  4. Lease expiry is computed from the apiserver-supplied renewTime, so
//     cross-client clock skew is not a concern. However, ExpiryInSeconds
//     should exceed the expected round-trip time or renewals may race the
//     deadline.
package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	kubeclient "github.com/dapr/components-contrib/common/authentication/kubernetes"
	"github.com/dapr/components-contrib/lock"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	// resourceIDAnnotation stores the original (pre-hash) ResourceID on the
	// Lease so operators inspecting leases can correlate them with application
	// resources.
	resourceIDAnnotation = "dapr.io/lock-resource-id"

	// leaseNameHashLen is the number of hex characters of the sha256 digest
	// used as the suffix in a Lease name. 40 hex chars = 160 bits, which is
	// comfortably collision-resistant for any realistic key space.
	leaseNameHashLen = 40
)

// kubernetesLock implements lock.Store using Kubernetes Lease resources.
type kubernetesLock struct {
	client    kubernetes.Interface
	md        kubernetesMetadata
	namespace string
	logger    logger.Logger

	// now is overridable for deterministic tests.
	now func() time.Time
}

// NewKubernetesLock returns a new Kubernetes lock store.
func NewKubernetesLock(logger logger.Logger) lock.Store {
	return &kubernetesLock{
		logger: logger,
		now:    time.Now,
	}
}

// InitLockStore parses metadata and initializes the Kubernetes client.
func (k *kubernetesLock) InitLockStore(_ context.Context, meta lock.Metadata) error {
	if err := k.md.parse(meta.Properties); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Namespace may come from metadata or the NAMESPACE env var (set by the
	// downward API in most Dapr deployments).
	if k.md.Namespace == "" {
		k.md.Namespace = os.Getenv("NAMESPACE")
	}
	if err := k.md.validateResolvedNamespace(); err != nil {
		return err
	}
	k.namespace = k.md.Namespace

	kubeconfigPath := k.md.KubeconfigPath
	if kubeconfigPath == "" {
		kubeconfigPath = kubeclient.GetKubeconfigPath(k.logger, os.Args)
	}
	client, err := kubeclient.GetKubeClient(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	k.client = client

	return nil
}

// TryLock attempts to acquire a lock by creating or taking over a Lease. It
// does not block: if the Lease is held by any live holder (including the same
// caller), it returns Success=false immediately. Expired Leases are taken
// over.
func (k *kubernetesLock) TryLock(ctx context.Context, req *lock.TryLockRequest) (*lock.TryLockResponse, error) {
	if err := validateTryLockRequest(req); err != nil {
		return &lock.TryLockResponse{}, err
	}
	leaseName := k.leaseName(req.ResourceID)
	duration := req.ExpiryInSeconds
	owner := req.LockOwner
	now := metav1.NewMicroTime(k.now())

	existing, err := k.client.CoordinationV1().Leases(k.namespace).Get(ctx, leaseName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		created, createErr := k.client.CoordinationV1().Leases(k.namespace).Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      leaseName,
				Namespace: k.namespace,
				Annotations: map[string]string{
					resourceIDAnnotation: req.ResourceID,
				},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &owner,
				LeaseDurationSeconds: &duration,
				AcquireTime:          &now,
				RenewTime:            &now,
			},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(createErr) {
			// Lost the race to another caller — not our lock.
			return &lock.TryLockResponse{Success: false}, nil
		}
		if createErr != nil {
			return &lock.TryLockResponse{}, fmt.Errorf("failed to create lease: %w", createErr)
		}
		_ = created
		return &lock.TryLockResponse{Success: true}, nil
	}
	if err != nil {
		return &lock.TryLockResponse{}, fmt.Errorf("failed to get lease: %w", err)
	}

	// Lease exists. Any live holder — including the same caller — blocks
	// re-acquisition, for parity with TTL-based locks (e.g. Redis SETNX).
	if leaseIsHeld(existing, now.Time) {
		return &lock.TryLockResponse{Success: false}, nil
	}

	// Lease is expired (or otherwise unheld) — take over.
	existing.Spec.HolderIdentity = &owner
	existing.Spec.LeaseDurationSeconds = &duration
	existing.Spec.AcquireTime = &now
	existing.Spec.RenewTime = &now
	if existing.Annotations == nil {
		existing.Annotations = map[string]string{}
	}
	existing.Annotations[resourceIDAnnotation] = req.ResourceID

	_, err = k.client.CoordinationV1().Leases(k.namespace).Update(ctx, existing, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		// Another client mutated the lease between our Get and Update.
		return &lock.TryLockResponse{Success: false}, nil
	}
	if err != nil {
		return &lock.TryLockResponse{}, fmt.Errorf("failed to update lease: %w", err)
	}
	return &lock.TryLockResponse{Success: true}, nil
}

// Unlock releases a lock by deleting the backing Lease. It verifies that the
// caller is the current holder; optimistic concurrency prevents racing a
// takeover that happens between Get and Delete.
func (k *kubernetesLock) Unlock(ctx context.Context, req *lock.UnlockRequest) (*lock.UnlockResponse, error) {
	if err := validateUnlockRequest(req); err != nil {
		return &lock.UnlockResponse{Status: lock.InternalError}, err
	}
	leaseName := k.leaseName(req.ResourceID)

	existing, err := k.client.CoordinationV1().Leases(k.namespace).Get(ctx, leaseName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &lock.UnlockResponse{Status: lock.LockDoesNotExist}, nil
	}
	if err != nil {
		return &lock.UnlockResponse{Status: lock.InternalError}, fmt.Errorf("failed to get lease: %w", err)
	}

	// If the lease has expired, treat it as if it no longer exists — parity
	// with TTL-based locks where expired keys disappear.
	if !leaseIsHeld(existing, k.now()) {
		return &lock.UnlockResponse{Status: lock.LockDoesNotExist}, nil
	}

	if existing.Spec.HolderIdentity == nil || *existing.Spec.HolderIdentity != req.LockOwner {
		return &lock.UnlockResponse{Status: lock.LockBelongsToOthers}, nil
	}

	// Guard Delete with UID + resourceVersion so a concurrent takeover cannot
	// cause us to delete someone else's lease.
	uid := existing.UID
	rv := existing.ResourceVersion
	preconditions := metav1.Preconditions{
		UID:             &uid,
		ResourceVersion: &rv,
	}
	err = k.client.CoordinationV1().Leases(k.namespace).Delete(ctx, leaseName, metav1.DeleteOptions{Preconditions: &preconditions})
	if apierrors.IsNotFound(err) {
		return &lock.UnlockResponse{Status: lock.LockDoesNotExist}, nil
	}
	if apierrors.IsConflict(err) {
		// Precondition failed — someone took over between our Get and Delete.
		return &lock.UnlockResponse{Status: lock.LockBelongsToOthers}, nil
	}
	if err != nil {
		return &lock.UnlockResponse{Status: lock.InternalError}, fmt.Errorf("failed to delete lease: %w", err)
	}
	return &lock.UnlockResponse{Status: lock.Success}, nil
}

// Close is a no-op: the kubernetes client has no persistent resources to release.
func (k *kubernetesLock) Close() error {
	return nil
}

// GetComponentMetadata returns the component metadata schema.
func (k *kubernetesLock) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := kubernetesMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.LockStoreType)
	return
}

// leaseName derives a DNS-1123-compliant Lease name from an arbitrary
// ResourceID. Hashing ensures names are valid and length-bounded regardless
// of what the caller provides.
func (k *kubernetesLock) leaseName(resourceID string) string {
	sum := sha256.Sum256([]byte(resourceID))
	suffix := hex.EncodeToString(sum[:])[:leaseNameHashLen]
	return k.md.LeaseNamePrefix + suffix
}

// leaseIsHeld reports whether a Lease is currently held by a live holder.
// A Lease is considered unheld if it has no holder, no duration, no renewTime,
// or if renewTime + duration is in the past.
func leaseIsHeld(lease *coordinationv1.Lease, now time.Time) bool {
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return false
	}
	if lease.Spec.LeaseDurationSeconds == nil || lease.Spec.RenewTime == nil {
		return false
	}
	expiry := lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
	return now.Before(expiry)
}

func validateTryLockRequest(req *lock.TryLockRequest) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if req.ResourceID == "" {
		return errors.New("resourceId is required")
	}
	if req.LockOwner == "" {
		return errors.New("lockOwner is required")
	}
	if req.ExpiryInSeconds <= 0 {
		return errors.New("expiryInSeconds must be greater than zero")
	}
	return nil
}

func validateUnlockRequest(req *lock.UnlockRequest) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if req.ResourceID == "" {
		return errors.New("resourceId is required")
	}
	if req.LockOwner == "" {
		return errors.New("lockOwner is required")
	}
	return nil
}

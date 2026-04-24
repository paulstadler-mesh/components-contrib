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

package kubernetes

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/dapr/components-contrib/lock"
	"github.com/dapr/kit/logger"
)

const (
	testNamespace  = "test-ns"
	testOwner      = "owner-a"
	testOtherOwner = "owner-b"
)

// newTestStore builds a kubernetesLock wired to a fake clientset with a fixed
// clock so expiry calculations are deterministic.
func newTestStore(t *testing.T, fixedNow time.Time, objects ...runtime.Object) (*kubernetesLock, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	k := &kubernetesLock{
		client:    client,
		md:        kubernetesMetadata{Namespace: testNamespace, LeaseNamePrefix: defaultLeaseNamePrefix},
		namespace: testNamespace,
		logger:    logger.NewLogger("test"),
		now:       func() time.Time { return fixedNow },
	}
	return k, client
}

func ptrStr(s string) *string { return &s }
func ptrI32(i int32) *int32   { return &i }

// getLease returns the Lease object the store would use for the given ResourceID.
func (k *kubernetesLock) getLease(t *testing.T, resourceID string) *coordinationv1.Lease {
	t.Helper()
	lease, err := k.client.CoordinationV1().Leases(k.namespace).Get(context.Background(), k.leaseName(resourceID), metav1.GetOptions{})
	require.NoError(t, err)
	return lease
}

func TestMetadataParse(t *testing.T) {
	t.Run("defaults applied", func(t *testing.T) {
		m := kubernetesMetadata{}
		require.NoError(t, m.parse(map[string]string{"namespace": "default"}))
		assert.Equal(t, defaultLeaseNamePrefix, m.LeaseNamePrefix)
	})

	t.Run("invalid prefix rejected", func(t *testing.T) {
		m := kubernetesMetadata{}
		err := m.parse(map[string]string{"leaseNamePrefix": "UPPER_CASE!"})
		require.Error(t, err)
	})

	t.Run("invalid namespace rejected", func(t *testing.T) {
		m := kubernetesMetadata{}
		err := m.parse(map[string]string{"namespace": "NotValid"})
		require.Error(t, err)
	})

	t.Run("prefix too long rejected", func(t *testing.T) {
		m := kubernetesMetadata{}
		long := make([]byte, maxLeaseNamePrefixLen+1)
		for i := range long {
			long[i] = 'a'
		}
		err := m.parse(map[string]string{"leaseNamePrefix": string(long)})
		require.Error(t, err)
	})

	t.Run("missing namespace fails validation", func(t *testing.T) {
		m := kubernetesMetadata{}
		require.NoError(t, m.parse(map[string]string{}))
		require.Error(t, m.validateResolvedNamespace())
	})
}

func TestLeaseNameIsDNS1123(t *testing.T) {
	k := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	// Include characters that are not valid in a DNS-1123 name to confirm
	// hashing normalizes arbitrary input.
	for _, id := range []string{"simple", "MixedCase", "with/slash", "with space", "üñíçødé", ""} {
		name := k.leaseName(id)
		errs := validation.IsDNS1123Subdomain(name)
		assert.Empty(t, errs, "lease name %q for ResourceID %q failed DNS-1123 validation: %v", name, id, errs)
	}
}

func TestLeaseNameIsStable(t *testing.T) {
	k := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	a := k.leaseName("my-resource")
	b := k.leaseName("my-resource")
	c := k.leaseName("my-resource-other")
	assert.Equal(t, a, b)
	assert.NotEqual(t, a, c)
}

func TestTryLock_CreateWhenAbsent(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	k, _ := newTestStore(t, now)

	resp, err := k.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      "resource-1",
		LockOwner:       testOwner,
		ExpiryInSeconds: 30,
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	lease := k.getLease(t, "resource-1")
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.Equal(t, testOwner, *lease.Spec.HolderIdentity)
	require.NotNil(t, lease.Spec.LeaseDurationSeconds)
	assert.Equal(t, int32(30), *lease.Spec.LeaseDurationSeconds)
	assert.Equal(t, "resource-1", lease.Annotations[resourceIDAnnotation])
}

func TestTryLock_FailsWhenHeldByAnother(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      (&kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}).leaseName("resource-2"),
			Namespace: testNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrStr(testOtherOwner),
			LeaseDurationSeconds: ptrI32(60),
			RenewTime:            metaMicroTimePtr(now.Add(-5 * time.Second)), // 55s remaining
		},
	}
	k, _ := newTestStore(t, now, existing)

	resp, err := k.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      "resource-2",
		LockOwner:       testOwner,
		ExpiryInSeconds: 30,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)

	// Holder should remain unchanged.
	lease := k.getLease(t, "resource-2")
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.Equal(t, testOtherOwner, *lease.Spec.HolderIdentity)
}

func TestTryLock_TakesOverExpiredLease(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	prefix := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prefix.leaseName("resource-3"),
			Namespace: testNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrStr(testOtherOwner),
			LeaseDurationSeconds: ptrI32(10),
			// Renewed 60s ago with a 10s duration — long since expired.
			RenewTime: metaMicroTimePtr(now.Add(-60 * time.Second)),
		},
	}
	k, _ := newTestStore(t, now, existing)

	resp, err := k.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      "resource-3",
		LockOwner:       testOwner,
		ExpiryInSeconds: 30,
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	lease := k.getLease(t, "resource-3")
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.Equal(t, testOwner, *lease.Spec.HolderIdentity)
}

func TestTryLock_FailsWhenSameOwnerAlreadyHolds(t *testing.T) {
	// TryLock refuses re-acquisition regardless of holder identity, for parity
	// with TTL-based locks (Redis SETNX semantics).
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	prefix := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prefix.leaseName("resource-4"),
			Namespace: testNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrStr(testOwner),
			LeaseDurationSeconds: ptrI32(60),
			RenewTime:            metaMicroTimePtr(now.Add(-5 * time.Second)),
		},
	}
	k, _ := newTestStore(t, now, existing)

	resp, err := k.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      "resource-4",
		LockOwner:       testOwner,
		ExpiryInSeconds: 30,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)

	// Existing lease should be unchanged.
	lease := k.getLease(t, "resource-4")
	require.NotNil(t, lease.Spec.LeaseDurationSeconds)
	assert.Equal(t, int32(60), *lease.Spec.LeaseDurationSeconds)
}

func TestTryLock_CreateRaceReturnsFailure(t *testing.T) {
	// Simulate another client creating the Lease between our Get and Create.
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	k, client := newTestStore(t, now)

	client.PrependReactor("create", "leases", func(_ clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "race")
	})

	resp, err := k.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      "resource-race",
		LockOwner:       testOwner,
		ExpiryInSeconds: 30,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)
}

func TestTryLock_UpdateConflictReturnsFailure(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	prefix := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prefix.leaseName("resource-conflict"),
			Namespace: testNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrStr(testOwner),
			LeaseDurationSeconds: ptrI32(60),
			RenewTime:            metaMicroTimePtr(now.Add(-1 * time.Second)),
		},
	}
	k, client := newTestStore(t, now, existing)

	client.PrependReactor("update", "leases", func(_ clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "resource-conflict", errors.New("conflict"))
	})

	resp, err := k.TryLock(context.Background(), &lock.TryLockRequest{
		ResourceID:      "resource-conflict",
		LockOwner:       testOwner,
		ExpiryInSeconds: 30,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)
}

func TestTryLock_InvalidRequest(t *testing.T) {
	k, _ := newTestStore(t, time.Now())
	cases := map[string]*lock.TryLockRequest{
		"no resource": {LockOwner: testOwner, ExpiryInSeconds: 10},
		"no owner":    {ResourceID: "r", ExpiryInSeconds: 10},
		"zero expiry": {ResourceID: "r", LockOwner: testOwner},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := k.TryLock(context.Background(), req)
			require.Error(t, err)
		})
	}
}

func TestUnlock_SuccessWhenOwner(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	prefix := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prefix.leaseName("u1"),
			Namespace: testNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrStr(testOwner),
			LeaseDurationSeconds: ptrI32(30),
			RenewTime:            metaMicroTimePtr(now.Add(-1 * time.Second)),
		},
	}
	k, _ := newTestStore(t, now, existing)

	resp, err := k.Unlock(context.Background(), &lock.UnlockRequest{
		ResourceID: "u1",
		LockOwner:  testOwner,
	})
	require.NoError(t, err)
	assert.Equal(t, lock.Success, resp.Status)

	_, err = k.client.CoordinationV1().Leases(testNamespace).Get(context.Background(), prefix.leaseName("u1"), metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestUnlock_LockDoesNotExist(t *testing.T) {
	k, _ := newTestStore(t, time.Now())

	resp, err := k.Unlock(context.Background(), &lock.UnlockRequest{
		ResourceID: "missing",
		LockOwner:  testOwner,
	})
	require.NoError(t, err)
	assert.Equal(t, lock.LockDoesNotExist, resp.Status)
}

func TestUnlock_LockBelongsToOthers(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	prefix := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prefix.leaseName("u2"),
			Namespace: testNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrStr(testOtherOwner),
			LeaseDurationSeconds: ptrI32(30),
			RenewTime:            metaMicroTimePtr(now.Add(-1 * time.Second)),
		},
	}
	k, _ := newTestStore(t, now, existing)

	resp, err := k.Unlock(context.Background(), &lock.UnlockRequest{
		ResourceID: "u2",
		LockOwner:  testOwner,
	})
	require.NoError(t, err)
	assert.Equal(t, lock.LockBelongsToOthers, resp.Status)
}

func TestUnlock_ExpiredLeaseTreatedAsMissing(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	prefix := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prefix.leaseName("u3"),
			Namespace: testNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrStr(testOwner),
			LeaseDurationSeconds: ptrI32(10),
			RenewTime:            metaMicroTimePtr(now.Add(-60 * time.Second)),
		},
	}
	k, _ := newTestStore(t, now, existing)

	resp, err := k.Unlock(context.Background(), &lock.UnlockRequest{
		ResourceID: "u3",
		LockOwner:  testOwner,
	})
	require.NoError(t, err)
	assert.Equal(t, lock.LockDoesNotExist, resp.Status)
}

func TestUnlock_DeleteConflictReturnsBelongsToOthers(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	prefix := &kubernetesLock{md: kubernetesMetadata{LeaseNamePrefix: defaultLeaseNamePrefix}}
	existing := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prefix.leaseName("u4"),
			Namespace: testNamespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrStr(testOwner),
			LeaseDurationSeconds: ptrI32(30),
			RenewTime:            metaMicroTimePtr(now.Add(-1 * time.Second)),
		},
	}
	k, client := newTestStore(t, now, existing)

	client.PrependReactor("delete", "leases", func(_ clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "u4", errors.New("precondition failed"))
	})

	resp, err := k.Unlock(context.Background(), &lock.UnlockRequest{
		ResourceID: "u4",
		LockOwner:  testOwner,
	})
	require.NoError(t, err)
	assert.Equal(t, lock.LockBelongsToOthers, resp.Status)
}

func TestLeaseIsHeld(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		spec coordinationv1.LeaseSpec
		want bool
	}{
		{"no holder", coordinationv1.LeaseSpec{}, false},
		{"empty holder", coordinationv1.LeaseSpec{HolderIdentity: ptrStr("")}, false},
		{"no duration", coordinationv1.LeaseSpec{HolderIdentity: ptrStr("a"), RenewTime: metaMicroTimePtr(now)}, false},
		{"no renewTime", coordinationv1.LeaseSpec{HolderIdentity: ptrStr("a"), LeaseDurationSeconds: ptrI32(10)}, false},
		{"live", coordinationv1.LeaseSpec{HolderIdentity: ptrStr("a"), LeaseDurationSeconds: ptrI32(30), RenewTime: metaMicroTimePtr(now.Add(-1 * time.Second))}, true},
		{"expired", coordinationv1.LeaseSpec{HolderIdentity: ptrStr("a"), LeaseDurationSeconds: ptrI32(10), RenewTime: metaMicroTimePtr(now.Add(-60 * time.Second))}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := leaseIsHeld(&coordinationv1.Lease{Spec: tc.spec}, now)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNewKubernetesLock(t *testing.T) {
	s := NewKubernetesLock(logger.NewLogger("test"))
	assert.NotNil(t, s)
}

func metaMicroTimePtr(t time.Time) *metav1.MicroTime {
	mt := metav1.NewMicroTime(t)
	return &mt
}

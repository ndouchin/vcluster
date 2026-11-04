package leaderelection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestHoldsLease(t *testing.T) {
	assert.Assert(t, holdsLease(nil), "without leader election the lease is always held")

	const (
		passUpdates int32 = iota
		stallUpdates
		rejectUpdates
	)
	leaseName := "vcluster-test-controller"
	conflict := kerrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, leaseName, errors.New("lease taken over"))

	client := fake.NewClientset()
	var updates atomic.Int32
	release := make(chan struct{})
	releaseStalledUpdate := sync.OnceFunc(func() { close(release) })
	client.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		switch updates.Load() {
		case stallUpdates:
			<-release
			return true, nil, conflict
		case rejectUpdates:
			return true, nil, conflict
		}
		return false, nil, nil
	})

	leading := make(chan struct{})
	state, err := newLeaderState(leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Namespace: "test", Name: leaseName},
			Client:     client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: "replica-0"},
		},
		LeaseDuration: time.Second,
		RenewDeadline: 800 * time.Millisecond,
		RetryPeriod:   100 * time.Millisecond,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) { close(leading) },
			OnStoppedLeading: func() {},
		},
	})
	assert.NilError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		state.elector.Run(ctx)
	}()
	defer func() {
		cancel()
		<-stopped
	}()
	defer releaseStalledUpdate()

	select {
	case <-leading:
	case <-time.After(10 * time.Second):
		t.Fatal("replica did not acquire the lease")
	}
	assert.Assert(t, holdsLease(state), "a leader that renews its lease holds it")

	// a renewal stuck for longer than the lease duration stands in for a paused process
	updates.Store(stallUpdates)
	time.Sleep(1500 * time.Millisecond)
	assert.Assert(t, state.elector.IsLeader(), "client-go still reports the stalled replica as leader")
	assert.Assert(t, !holdsLease(state), "a leader whose last renewal is older than the renew deadline must not trust its lease")

	// on resume the renewal fails after client-go re-read the lease record, which refreshes its observed time
	updates.Store(rejectUpdates)
	releaseStalledUpdate()
	time.Sleep(300 * time.Millisecond)
	assert.Assert(t, state.elector.IsLeader(), "the re-read record still names the stalled replica")
	assert.Assert(t, !holdsLease(state), "reading the lease record must not count as a renewal")
}

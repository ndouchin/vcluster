package leaderelection

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestHoldsLease(t *testing.T) {
	assert.Assert(t, holdsLease(nil), "without leader election the lease is always held")

	client := fake.NewClientset()
	var stallRenewals atomic.Bool
	release := make(chan struct{})
	client.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if stallRenewals.Load() {
			<-release
		}
		return false, nil, nil
	})

	leading := make(chan struct{})
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Namespace: "test", Name: "vcluster-test-controller"},
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
		le.Run(ctx)
	}()
	defer func() {
		cancel()
		<-stopped
	}()
	defer close(release)

	select {
	case <-leading:
	case <-time.After(10 * time.Second):
		t.Fatal("replica did not acquire the lease")
	}
	assert.Assert(t, holdsLease(le), "a leader that renews its lease holds it")

	// a renewal stuck for longer than the lease duration stands in for a paused process
	stallRenewals.Store(true)
	time.Sleep(1500 * time.Millisecond)
	assert.Assert(t, le.IsLeader(), "client-go still reports the stalled replica as leader")
	assert.Assert(t, !holdsLease(le), "a leader whose last renewal is older than the lease duration must not trust its lease")
}

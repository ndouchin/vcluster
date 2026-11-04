package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/scheme"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/util/loghelper"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type countingFakeSyncer struct {
	calls int
}

func (s *countingFakeSyncer) Name() string { return "counting" }

func (s *countingFakeSyncer) Resource() client.Object { return &corev1.Node{} }

func (s *countingFakeSyncer) FakeSyncToVirtual(*synccontext.SyncContext, types.NamespacedName) (ctrl.Result, error) {
	s.calls++
	return ctrl.Result{}, nil
}

func (s *countingFakeSyncer) FakeSync(*synccontext.SyncContext, client.Object) (ctrl.Result, error) {
	s.calls++
	return ctrl.Result{}, nil
}

func TestFakeSyncerRequiresLease(t *testing.T) {
	for _, leaseHeld := range []bool{true, false} {
		syncer := &countingFakeSyncer{}
		controller := &fakeSyncer{
			syncer:        syncer,
			log:           loghelper.New("test"),
			virtualClient: testingutil.NewFakeClient(scheme.Scheme),
			holdsLease:    func() bool { return leaseHeld },
		}

		result, err := controller.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "node"}})
		assert.NilError(t, err)
		if leaseHeld {
			assert.Equal(t, syncer.calls, 1, "the fake syncer runs while the lease is held")
		} else {
			assert.Equal(t, syncer.calls, 0, "the fake syncer must not run without the lease")
			assert.Equal(t, result.RequeueAfter, time.Second)
		}
	}
}

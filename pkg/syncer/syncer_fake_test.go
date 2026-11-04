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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

type deletingFakeSyncer struct {
	countingFakeSyncer
}

func (s *deletingFakeSyncer) FakeSync(ctx *synccontext.SyncContext, vObj client.Object) (ctrl.Result, error) {
	return ctrl.Result{}, ctx.VirtualClient.Delete(ctx, vObj)
}

func TestFakeSyncerFencesDeletes(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}
	virtualClient := testingutil.NewFakeClient(scheme.Scheme, node.DeepCopy())
	leaseChecks := 0
	controller := &fakeSyncer{
		syncer:        &deletingFakeSyncer{},
		log:           loghelper.New("test"),
		virtualClient: virtualClient,
		holdsLease: func() bool {
			leaseChecks++
			return leaseChecks == 1
		},
	}

	_, err := controller.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: node.Name}})
	assert.ErrorContains(t, err, "leader lease could not be confirmed")
	assert.NilError(t, virtualClient.Get(context.Background(), client.ObjectKeyFromObject(node), &corev1.Node{}), "the node must survive a delete without the lease")
}

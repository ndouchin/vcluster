package syncer

import (
	"context"
	"testing"

	"github.com/loft-sh/vcluster/pkg/scheme"
	testingutil "github.com/loft-sh/vcluster/pkg/util/testing"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestFenceDeletes(t *testing.T) {
	for _, leaseHeld := range []bool{true, false} {
		ctx := context.Background()
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"}}
		inner := testingutil.NewFakeClient(scheme.Scheme, secret.DeepCopy())
		fenced := fenceDeletes(inner, func() bool { return leaseHeld })

		assert.NilError(t, fenced.Get(ctx, client.ObjectKeyFromObject(secret), &corev1.Secret{}), "reads pass through")

		err := fenced.Delete(ctx, secret.DeepCopy())
		getErr := inner.Get(ctx, client.ObjectKeyFromObject(secret), &corev1.Secret{})
		if leaseHeld {
			assert.NilError(t, err)
			assert.Assert(t, kerrors.IsNotFound(getErr), "delete goes through while the lease is held")
		} else {
			assert.ErrorContains(t, err, "leader lease could not be confirmed")
			assert.NilError(t, getErr, "the object must survive a delete without the lease")
			assert.ErrorContains(t, fenced.DeleteAllOf(ctx, &corev1.Secret{}, client.InNamespace("ns")), "leader lease could not be confirmed")
			assert.NilError(t, inner.Get(ctx, client.ObjectKeyFromObject(secret), &corev1.Secret{}), "the object must survive a delete-all without the lease")
		}
	}

	assert.Assert(t, fenceDeletes(nil, func() bool { return true }) == nil, "a nil client stays nil")
}

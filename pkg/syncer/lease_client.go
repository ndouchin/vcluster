package syncer

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// leaseFencedClient refuses deletes while the leader lease can't be confirmed. Many syncers delete
// through the sync context's clients without the patcher, and a reconcile that was already running
// when the replica stalled would otherwise delete from stale caches after it resumes.
type leaseFencedClient struct {
	client.Client

	holdsLease func() bool
}

func fenceDeletes(c client.Client, holdsLease func() bool) client.Client {
	if c == nil || holdsLease == nil {
		return c
	}

	return &leaseFencedClient{Client: c, holdsLease: holdsLease}
}

func (c *leaseFencedClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if !c.holdsLease() {
		return fmt.Errorf("refuse to delete %T %s/%s, because the leader lease could not be confirmed", obj, obj.GetNamespace(), obj.GetName())
	}

	return c.Client.Delete(ctx, obj, opts...)
}

func (c *leaseFencedClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	if !c.holdsLease() {
		return fmt.Errorf("refuse to delete all %T, because the leader lease could not be confirmed", obj)
	}

	return c.Client.DeleteAllOf(ctx, obj, opts...)
}

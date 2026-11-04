package syncer

import (
	"context"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/constants"
	"github.com/loft-sh/vcluster/pkg/leaderelection"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	syncertypes "github.com/loft-sh/vcluster/pkg/syncer/types"
	"github.com/loft-sh/vcluster/pkg/util/loghelper"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controller2 "sigs.k8s.io/controller-runtime/pkg/controller"
)

func RegisterFakeSyncer(ctx *synccontext.RegisterContext, syncer syncertypes.FakeSyncer) error {
	controller := &fakeSyncer{
		syncer:         syncer,
		log:            loghelper.New(syncer.Name()),
		physicalClient: ctx.HostManager.GetClient(),

		currentNamespace:       ctx.CurrentNamespace,
		currentNamespaceClient: ctx.CurrentNamespaceClient,

		mappings: ctx.Mappings,

		config: ctx.Config,

		virtualClient: ctx.VirtualManager.GetClient(),

		holdsLease: leaderelection.HoldsLease,
	}

	return controller.Register(ctx)
}

type fakeSyncer struct {
	syncer syncertypes.FakeSyncer
	log    loghelper.Logger

	physicalClient client.Client

	currentNamespace       string
	currentNamespaceClient client.Client

	mappings synccontext.MappingsRegistry

	config *config.VirtualClusterConfig

	virtualClient client.Client

	holdsLease func() bool
}

func (r *fakeSyncer) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if !r.holdsLease() {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	log := loghelper.NewFromExisting(r.log.Base(), req.Name)
	syncContext := &synccontext.SyncContext{
		Context:                ctx,
		Log:                    log,
		Config:                 r.config,
		HostClient:             fenceDeletes(r.physicalClient, r.holdsLease),
		CurrentNamespace:       r.currentNamespace,
		CurrentNamespaceClient: fenceDeletes(r.currentNamespaceClient, r.holdsLease),
		VirtualClient:          fenceDeletes(r.virtualClient, r.holdsLease),
		Mappings:               r.mappings,
		HoldsLease:             r.holdsLease,
	}

	// check if we should skip reconcile
	lifecycle, ok := r.syncer.(syncertypes.Starter)
	if ok {
		skip, err := lifecycle.ReconcileStart(syncContext, req)
		defer lifecycle.ReconcileEnd()
		if skip || err != nil {
			return ctrl.Result{}, err
		}
	}

	// get virtual object
	vObj := r.syncer.Resource()
	err := r.virtualClient.Get(ctx, req.NamespacedName, vObj)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}

		return r.syncer.FakeSyncToVirtual(syncContext, req.NamespacedName)
	}

	// check if we should skip resource
	if vObj != nil && vObj.GetLabels() != nil && vObj.GetLabels()[translate.ControllerLabel] != "" {
		return ctrl.Result{}, nil
	}

	// update object
	return r.syncer.FakeSync(syncContext, vObj)
}

func (r *fakeSyncer) Register(ctx *synccontext.RegisterContext) error {
	controller := ctrl.NewControllerManagedBy(ctx.VirtualManager).
		WithOptions(controller2.Options{
			MaxConcurrentReconciles: 10,
			CacheSyncTimeout:        constants.DefaultCacheSyncTimeout,
		}).
		Named(r.syncer.Name()).
		For(r.syncer.Resource())
	var err error
	modifier, ok := r.syncer.(syncertypes.ControllerModifier)
	if ok {
		controller, err = modifier.ModifyController(ctx, controller)
		if err != nil {
			return err
		}
	}
	return controller.Complete(r)
}

package leaderelection

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"github.com/loft-sh/vcluster/pkg/telemetry"
	"github.com/loft-sh/vcluster/pkg/util/osutil"
	"github.com/loft-sh/vcluster/pkg/util/translate"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

var (
	elector           atomic.Pointer[leaderelection.LeaderElector]
	reportedLeaseLoss atomic.Bool
)

// HoldsLease reports whether this replica can trust that it still holds the leader lease. client-go
// doesn't fence a former leader: a replica that stalls for longer than the lease duration resumes and
// keeps reconciling until its renew deadline passes, while another replica may already lead. Without
// leader election (a single replica) it always returns true.
func HoldsLease() bool {
	if holdsLease(elector.Load()) {
		if reportedLeaseLoss.CompareAndSwap(true, false) {
			klog.Info("Resume reconciling, because the leader lease is confirmed again")
		}
		return true
	}

	if reportedLeaseLoss.CompareAndSwap(false, true) {
		klog.Info("Stop reconciling, because the leader lease could not be confirmed")
	}
	return false
}

func holdsLease(le *leaderelection.LeaderElector) bool {
	return le == nil || (le.IsLeader() && le.Check(0) == nil)
}

func StartLeaderElection(ctx *synccontext.ControllerContext, scheme *runtime.Scheme, run func() error) error {
	localConfig := ctx.HostManager.GetConfig()

	// create the event recorder
	recorderClient, err := kubernetes.NewForConfig(localConfig)
	if err != nil {
		return errors.Wrap(err, "create kubernetes client")
	}
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(func(format string, args ...interface{}) { klog.Infof(format, args...) })
	eventBroadcaster.StartRecordingToSink(&clientv1.EventSinkImpl{Interface: recorderClient.CoreV1().Events(ctx.Config.HostNamespace)})
	recorder := eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "vcluster"})

	// create the leader election client
	leaderElectionClient, err := kubernetes.NewForConfig(rest.AddUserAgent(localConfig, "leader-election"))
	if err != nil {
		return errors.Wrap(err, "create leader election client")
	}

	// Identity used to distinguish between multiple controller manager instances
	id, err := os.Hostname()
	if err != nil {
		return err
	}

	// Lock required for leader election
	rl, err := resourcelock.New(
		resourcelock.LeasesResourceLock,
		ctx.Config.HostNamespace,
		translate.SafeConcatName("vcluster", translate.VClusterName, "controller"),
		leaderElectionClient.CoreV1(),
		leaderElectionClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      id + "-external-vcluster-controller",
			EventRecorder: recorder,
		},
	)
	if err != nil {
		return err
	}

	// try and become the leader and start controller manager loops
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: time.Duration(ctx.Config.ControlPlane.StatefulSet.HighAvailability.LeaseDuration) * time.Second,
		RenewDeadline: time.Duration(ctx.Config.ControlPlane.StatefulSet.HighAvailability.RenewDeadline) * time.Second,
		RetryPeriod:   time.Duration(ctx.Config.ControlPlane.StatefulSet.HighAvailability.RetryPeriod) * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(_ context.Context) {
				klog.Info("Acquired leadership and run vcluster in leader mode")

				// start vcluster in leader mode
				err = run()
				if err != nil {
					klog.Error(err)
					osutil.Exit(1)
				}
			},
			OnStoppedLeading: func() {
				klog.Info("leader election lost")

				// vcluster_error
				telemetry.CollectorControlPlane.RecordError(ctx, ctx.Config, telemetry.WarningSeverity, fmt.Errorf("leader election lost"))
				telemetry.CollectorControlPlane.Flush()

				osutil.Exit(1)
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "create leader elector")
	}

	elector.Store(le)
	le.Run(ctx)

	return nil
}

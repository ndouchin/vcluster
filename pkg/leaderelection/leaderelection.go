package leaderelection

import (
	"context"
	"fmt"
	"os"
	"sync"
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
	leader            atomic.Pointer[leaderState]
	reportedLeaseLoss atomic.Bool
	leaseLogLock      sync.Mutex
)

type leaderState struct {
	elector       *leaderelection.LeaderElector
	lock          *renewalRecordingLock
	renewDeadline time.Duration
}

// HoldsLease reports whether this replica can trust that it still holds the leader lease: it is the
// observed holder and its last successful renewal started less than the renew deadline ago. client-go
// doesn't fence a former leader: a replica that stalls for longer than the lease duration resumes and
// keeps reconciling until its renew deadline passes, while another replica may already lead. It also
// turns false when renewals keep failing, up to one retry period before client-go gives the lease up.
// Without leader election (a single replica) it always returns true.
func HoldsLease() bool {
	holds := holdsLease(leader.Load())
	if holds != reportedLeaseLoss.Load() {
		return holds
	}

	// recompute under the lock, so concurrent callers can't log a transition out of order
	leaseLogLock.Lock()
	defer leaseLogLock.Unlock()

	holds = holdsLease(leader.Load())
	if holds && reportedLeaseLoss.Load() {
		reportedLeaseLoss.Store(false)
		klog.Info("Resume reconciling, because the leader lease is confirmed again")
	} else if !holds && !reportedLeaseLoss.Load() {
		reportedLeaseLoss.Store(true)
		klog.Info("Stop reconciling, because the leader lease could not be confirmed")
	}
	return holds
}

func holdsLease(state *leaderState) bool {
	if state == nil {
		return true
	}

	lastRenewal := state.lock.lastRenewal.Load()
	return state.elector.IsLeader() && lastRenewal != nil && time.Since(*lastRenewal) < state.renewDeadline
}

func newLeaderState(config leaderelection.LeaderElectionConfig) (*leaderState, error) {
	lock := &renewalRecordingLock{Interface: config.Lock}
	config.Lock = lock
	elector, err := leaderelection.NewLeaderElector(config)
	if err != nil {
		return nil, err
	}

	return &leaderState{elector: elector, lock: lock, renewDeadline: config.RenewDeadline}, nil
}

// renewalRecordingLock records when this replica's last successful acquisition or renewal started.
// Other replicas date the lease from when they observe that write, which is never earlier. client-go's
// own observed time moves when the response arrives, and also when the record is merely read before a
// renewal that then fails.
type renewalRecordingLock struct {
	resourcelock.Interface

	lastRenewal atomic.Pointer[time.Time]
}

func (l *renewalRecordingLock) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	return l.recordWrite(record, func() error { return l.Interface.Create(ctx, record) })
}

func (l *renewalRecordingLock) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	return l.recordWrite(record, func() error { return l.Interface.Update(ctx, record) })
}

func (l *renewalRecordingLock) recordWrite(record resourcelock.LeaderElectionRecord, write func() error) error {
	start := time.Now()
	if err := write(); err != nil {
		return err
	}

	if record.HolderIdentity == l.Identity() {
		l.lastRenewal.Store(&start)
	}
	return nil
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
	state, err := newLeaderState(leaderelection.LeaderElectionConfig{
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

	leader.Store(state)
	state.elector.Run(ctx)

	return nil
}

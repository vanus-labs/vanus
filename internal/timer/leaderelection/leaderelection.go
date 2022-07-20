package leaderelection

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"

	rl "github.com/linkall-labs/vanus/internal/timer/leaderelection/resourcelock"
	"k8s.io/utils/clock"

	"github.com/linkall-labs/vanus/observability/log"
)

const (
	JitterFactor = 1.2
)

var (
	le *LeaderElector
)

type LeaderElectionConfig struct {
	// Lock is the resource that will be used for locking
	Lock rl.Interface

	// LeaseDuration is the duration that non-leader candidates will
	// wait to force acquire leadership. This is measured against time of
	// last observed ack.
	//
	// A client needs to wait a full LeaseDuration without observing a change to
	// the record before it can attempt to take over. When all clients are
	// shutdown and a new set of clients are started with different names against
	// the same leader record, they must wait the full LeaseDuration before
	// attempting to acquire the lease. Thus LeaseDuration should be as short as
	// possible (within your tolerance for clock skew rate) to avoid a possible
	// long waits in the scenario.
	//
	// Core clients default this value to 15 seconds.
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting master will retry
	// refreshing leadership before giving up.
	//
	// Core clients default this value to 10 seconds.
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaderElector clients should wait
	// between tries of actions.
	//
	// Core clients default this value to 2 seconds.
	RetryPeriod time.Duration

	// Callbacks are callbacks that are triggered during certain lifecycle
	// events of the LeaderElector
	Callbacks LeaderCallbacks

	// WatchDog is the associated health checker
	// WatchDog may be null if it's not needed/configured.
	// WatchDog *HealthzAdaptor

	// ReleaseOnCancel should be set true if the lock should be released
	// when the run context is cancelled. If you set this to true, you must
	// ensure all code guarded by this lease has successfully completed
	// prior to cancelling the context, or you may have two processes
	// simultaneously acting on the critical path.
	ReleaseOnCancel bool

	// Name is the name of the resource lock for debugging
	Name string
}

// LeaderCallbacks are callbacks that are triggered during certain
// lifecycle events of the LeaderElector. These are invoked asynchronously.
//
// possible future callbacks:
//  * OnChallenge()
type LeaderCallbacks struct {
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(context.Context)
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func(context.Context)
	// OnNewLeader is called when the client observes a leader that is
	// not the previously observed leader. This includes the first observed
	// leader when the client starts.
	OnNewLeader func(identity string)
}

// LeaderElector is a leader election client.
type LeaderElector struct {
	config LeaderElectionConfig
	// internal bookkeeping
	observedRecord    rl.LeaderElectionRecord
	observedRawRecord []byte
	observedTime      time.Time
	// used to implement OnNewLeader(), may lag slightly from the
	// value observedRecord.HolderIdentity if the transition has
	// not yet been reported.
	reportedLeader string

	// clock is wrapper around time to allow for less flaky testing
	clock clock.Clock

	// used to lock the observedRecord
	observedRecordLock sync.Mutex

	// metrics leaderMetricsAdapter
}

// func leaderElectAndRun(c *config.CompletedConfig, lockIdentity string, electionChecker *leaderelection.HealthzAdaptor, resourceLock string, leaseName string, callbacks leaderelection.LeaderCallbacks)  {
func LeaderElectAndRun(ctx context.Context, c *Config, callbacks LeaderCallbacks) {
	hostname, _ := os.Hostname()
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	c.Identity = fmt.Sprintf("%s_%s", hostname, string(uuid.NewUUID()))
	rl, err := rl.New(c.EtcdClient, c.Identity, c.ResourceLockName)
	if err != nil {
		log.Error(ctx, "create lock failed", map[string]interface{}{
			log.KeyError: err,
		})
	}

	RunOrDie(ctx, LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks:     callbacks,
		Name:          c.ResourceLockName,
	})

	// panic("unreachable")
}

func Instance() *LeaderElector {
	return le
}

// NewLeaderElector creates a LeaderElector from a LeaderElectionConfig
func NewLeaderElector(lec LeaderElectionConfig) (*LeaderElector, error) {
	if lec.LeaseDuration <= lec.RenewDeadline {
		return nil, fmt.Errorf("leaseDuration must be greater than renewDeadline")
	}
	if lec.RenewDeadline <= time.Duration(JitterFactor*float64(lec.RetryPeriod)) {
		return nil, fmt.Errorf("renewDeadline must be greater than retryPeriod*JitterFactor")
	}
	if lec.LeaseDuration < 1 {
		return nil, fmt.Errorf("leaseDuration must be greater than zero")
	}
	if lec.RenewDeadline < 1 {
		return nil, fmt.Errorf("renewDeadline must be greater than zero")
	}
	if lec.RetryPeriod < 1 {
		return nil, fmt.Errorf("retryPeriod must be greater than zero")
	}
	if lec.Callbacks.OnStartedLeading == nil {
		return nil, fmt.Errorf("OnStartedLeading callback must not be nil")
	}
	if lec.Callbacks.OnStoppedLeading == nil {
		return nil, fmt.Errorf("OnStoppedLeading callback must not be nil")
	}
	if lec.Lock == nil {
		return nil, fmt.Errorf("Lock must not be nil.")
	}

	// le := LeaderElector{
	// 	config: lec,
	// 	clock:  clock.RealClock{},
	// }
	return &LeaderElector{
		config: lec,
		clock:  clock.RealClock{},
	}, nil
}

// Run starts the leader election loop. Run will not return
// before leader election loop is stopped by ctx or it has
// stopped holding the leader lease
func (le *LeaderElector) Run(ctx context.Context) {
	defer runtime.HandleCrash()
	defer func() {
		le.config.Callbacks.OnStoppedLeading(ctx)
	}()

	if !le.acquire(ctx) {
		return // ctx signalled done
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go le.config.Callbacks.OnStartedLeading(ctx)
	le.renew(ctx)
}

// RunOrDie starts a client with the provided config or panics if the config
// fails to validate. RunOrDie blocks until leader election loop is
// stopped by ctx or it has stopped holding the leader lease
func RunOrDie(ctx context.Context, lec LeaderElectionConfig) {
	var err error
	le, err = NewLeaderElector(lec)
	if err != nil {
		panic(err)
	}
	le.Run(ctx)
}

// GetLeader returns the identity of the last observed leader or returns the empty string if
// no leader has yet been observed.
// This function is for informational purposes. (e.g. monitoring, logs, etc.)
func (le *LeaderElector) GetLeader() string {
	return le.getObservedRecord().HolderIdentity
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (le *LeaderElector) IsLeader() bool {
	return le.getObservedRecord().HolderIdentity == le.config.Lock.Identity()
}

// acquire loops calling tryAcquireOrRenew and returns true immediately when tryAcquireOrRenew succeeds.
// Returns false if ctx signals done.
func (le *LeaderElector) acquire(ctx context.Context) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	succeeded := false
	desc := le.config.Lock.Describe()
	log.Info(context.Background(), "attempting to acquire leader lease", map[string]interface{}{
		"lease": desc,
	})
	wait.JitterUntil(func() {
		succeeded = le.tryAcquireOrRenew(ctx)
		// le.maybeReportTransition()
		if !succeeded {
			log.Info(context.Background(), "failed to acquire lease", map[string]interface{}{
				"lease": desc,
			})
			return
		}
		// le.config.Lock.RecordEvent("became leader")
		log.Info(context.Background(), "successfully acquired lease", map[string]interface{}{
			"lease": desc,
		})
		cancel()
	}, le.config.RetryPeriod, JitterFactor, true, ctx.Done())
	return succeeded
}

// renew loops calling tryAcquireOrRenew and returns immediately when tryAcquireOrRenew fails or ctx signals done.
func (le *LeaderElector) renew(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait.Until(func() {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, le.config.RenewDeadline)
		defer timeoutCancel()
		err := wait.PollImmediateUntil(le.config.RetryPeriod, func() (bool, error) {
			return le.tryAcquireOrRenew(timeoutCtx), nil
		}, timeoutCtx.Done())

		// le.maybeReportTransition()
		desc := le.config.Lock.Describe()
		if err == nil {
			log.Info(context.Background(), "successfully renewed lease", map[string]interface{}{
				"lease": desc,
			})
			return
		}
		// le.config.Lock.RecordEvent("stopped leading")
		log.Error(context.Background(), "failed to renew lease", map[string]interface{}{
			"lease":      desc,
			log.KeyError: err,
		})
		cancel()
	}, le.config.RetryPeriod, ctx.Done())

	// if we hold the lease, give it up
	if le.config.ReleaseOnCancel {
		le.release()
	}
}

// release attempts to release the leader lease if we have acquired it.
func (le *LeaderElector) release() bool {
	if !le.IsLeader() {
		return true
	}
	leaderElectionRecord := rl.LeaderElectionRecord{
		LeaderTransitions:    le.observedRecord.LeaderTransitions,
		LeaseDurationSeconds: 1,
		RenewTime:            time.Now(),
		AcquireTime:          time.Now(),
	}
	if err := le.config.Lock.Update(context.TODO(), leaderElectionRecord); err != nil {
		log.Error(context.Background(), "failed to release lock", map[string]interface{}{
			log.KeyError: err,
		})
		return false
	}

	le.setObservedRecord(&leaderElectionRecord)
	return true
}

// tryAcquireOrRenew tries to acquire a leader lease if it is not already acquired,
// else it tries to renew the lease if it has already been acquired. Returns true
// on success else returns false.
func (le *LeaderElector) tryAcquireOrRenew(ctx context.Context) bool {
	leaderElectionRecord := rl.LeaderElectionRecord{
		HolderIdentity:       le.config.Lock.Identity(),
		LeaseDurationSeconds: int(le.config.LeaseDuration / time.Second),
		RenewTime:            time.Now(),
		AcquireTime:          time.Now(),
	}

	// 1. obtain or create the ElectionRecord
	oldLeaderElectionRecord, oldLeaderElectionRawRecord, err := le.config.Lock.Get(ctx)
	if err != nil {
		// if !errors.IsNotFound(err) {
		// 	log.Error(context.Background(), "error retrieving resource lock", map[string]interface{}{
		// 		"lock":       le.config.Lock.Describe(),
		// 		log.KeyError: err,
		// 	})
		// 	return false
		// }
		if err = le.config.Lock.Create(ctx, leaderElectionRecord); err != nil {
			log.Error(context.Background(), "error initially creating leader election record", map[string]interface{}{
				log.KeyError: err,
			})
			return false
		}

		le.setObservedRecord(&leaderElectionRecord)

		return true
	}

	// 2. Record obtained, check the Identity & Time
	if !bytes.Equal(le.observedRawRecord, oldLeaderElectionRawRecord) {
		le.setObservedRecord(oldLeaderElectionRecord)

		le.observedRawRecord = oldLeaderElectionRawRecord
	}
	if len(oldLeaderElectionRecord.HolderIdentity) > 0 &&
		// le.observedTime.Add(le.config.LeaseDuration).After(time.Now()) &&
		le.observedRecord.RenewTime.Add(le.config.LeaseDuration).After(time.Now()) &&
		!le.IsLeader() {
		log.Info(context.Background(), "lock is held and has not yet expired", map[string]interface{}{
			"holder": oldLeaderElectionRecord.HolderIdentity,
		})
		return false
	}

	// 3. We're going to try to update. The leaderElectionRecord is set to it's default
	// here. Let's correct it before updating.
	if le.IsLeader() {
		leaderElectionRecord.AcquireTime = oldLeaderElectionRecord.AcquireTime
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions
	} else {
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions + 1
	}

	// update the lock itself
	if err = le.config.Lock.Update(ctx, leaderElectionRecord); err != nil {
		log.Error(context.Background(), "Failed to update lock", map[string]interface{}{
			log.KeyError: err,
		})
		return false
	}

	le.setObservedRecord(&leaderElectionRecord)
	return true
}

// func (le *LeaderElector) maybeReportTransition() {
// 	if le.observedRecord.HolderIdentity == le.reportedLeader {
// 		return
// 	}
// 	le.reportedLeader = le.observedRecord.HolderIdentity
// 	if le.config.Callbacks.OnNewLeader != nil {
// 		go le.config.Callbacks.OnNewLeader(le.reportedLeader)
// 	}
// }

// setObservedRecord will set a new observedRecord and update observedTime to the current time.
// Protect critical sections with lock.
func (le *LeaderElector) setObservedRecord(observedRecord *rl.LeaderElectionRecord) {
	le.observedRecordLock.Lock()
	defer le.observedRecordLock.Unlock()

	le.observedRecord = *observedRecord
	le.observedTime = le.clock.Now()
}

// getObservedRecord returns observersRecord.
// Protect critical sections with lock.
func (le *LeaderElector) getObservedRecord() rl.LeaderElectionRecord {
	le.observedRecordLock.Lock()
	defer le.observedRecordLock.Unlock()

	return le.observedRecord
}

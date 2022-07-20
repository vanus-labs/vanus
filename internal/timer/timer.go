package timer

import (
	"context"
	"time"

	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/timer/leaderelection"
	"github.com/linkall-labs/vanus/internal/timer/timingwheel"
	"github.com/linkall-labs/vanus/observability/log"

	ce "github.com/cloudevents/sdk-go/v2"
	eventctl "github.com/linkall-labs/vanus/internal/timer/event"
	"github.com/linkall-labs/vanus/internal/timer/eventbus"
)

func (c *Config) Init() error {
	client, err := etcd.NewEtcdClientV3(c.EtcdEndpoints, c.MetadataConfig.KeyPrefix)
	if err != nil {
		return err
	}
	c.EtcdClient = client

	eventbus.Init(c.GetEventBusConfig())
	eventctl.Init(c.GetEventConfig())

	c.TimingWheelManager = timingwheel.NewTimingWheel(c.GetTimingWheelConfig())

	return nil
}

func (c *Config) Start(ctx context.Context) error {
	// 1. start timingwheel
	// tw := timingwheel.NewTimingWheel(c.GetTimingWheelConfig())
	if err := c.TimingWheelManager.Start(ctx); err != nil {
		log.Error(ctx, "start timer wheel failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}

	waitC := make(chan struct{})

	// 2. start leaderelection
	go leaderelection.LeaderElectAndRun(ctx, c.GetLeaderElectionConfig(), leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			log.Info(ctx, "leaderelection finish", nil)
			// todo something
			waitC <- struct{}{}
			c.TimingWheelManager.SetLeader(true)
		},
		OnStoppedLeading: func(ctx context.Context) {
			log.Info(ctx, "leaderelection lost", nil)
			// todo something
			c.TimingWheelManager.SetLeader(false)
		},
	})

	// just test
	time.Sleep(3 * time.Second)
	<-waitC

	e1 := ce.NewEvent()
	e1.SetID("jk1")
	e1.SetTime(time.Now().Add(15 * time.Second))
	e1.SetSource("quick-start")
	e1.SetType("examples")
	e1.SetExtension("xvanuseventbus", "quick-start")
	e1.SetData(ce.TextPlain, "Hello Vanus")
	c.TimingWheelManager.Add(&e1)

	// e2 := ce.NewEvent()
	// e2.SetID("25")
	// e2.SetTime(time.Now().Add(25 * time.Second))
	// e2.SetSource("quick-start")
	// e2.SetType("examples")
	// e2.SetExtension("xvanuseventbus", "quick-start")
	// e2.SetData(ce.TextPlain, "Hello Vanus")
	// c.TimingWheelManager.Add(&e2)

	return nil
}

func (c *Config) Stop() error {
	c.TimingWheelManager.Stop()
	return nil
}

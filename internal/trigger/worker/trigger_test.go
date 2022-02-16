package worker

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/primitive"
	"testing"
	"time"
)

func Test_e2e(t *testing.T) {
	tg := NewTrigger(&primitive.Subscription{
		ID:               "test",
		Source:           "human",
		Types:            []string{"aaa"},
		Config:           map[string]string{},
		Filters:          []primitive.SubscriptionFilter{{Exact: map[string]string{"type": "testType"}}},
		Sink:             "print",
		Protocol:         "vanus",
		ProtocolSettings: nil,
	})
	emit := 0
	pre := 0
	go func() {
		for {
			time.Sleep(time.Second)
			cur := emit
			t.Logf("TPS: %d", cur-pre)
			pre = cur
		}
	}()
	go func() {
		for {
			event := cloudevents.NewEvent()
			event.SetSource("manual")
			event.SetType("none")
			event.SetData(cloudevents.ApplicationJSON, map[string]string{"hello": "world"})
			tg.EventArrived(context.Background(), event)
			emit++
		}
	}()
	tg.Start(context.Background())

	time.Sleep(time.Hour)
	tg.Stop(context.Background())
}

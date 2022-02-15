package trigger

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/primitive"
	"testing"
	"time"
)

func TestManually(t *testing.T) {
	tg := NewTrigger(&primitive.Subscription{
		ID:               "test",
		Source:           "human",
		Types:            []string{"aaa"},
		Config:           map[string]string{},
		Filters:          nil,
		Sink:             "print",
		Protocol:         "vanus",
		ProtocolSettings: nil,
	})
	tg.Start(context.Background())
	event := cloudevents.NewEvent()
	event.DataAs("hello world")
	tg.EventArrived(context.Background(), &event)
	time.Sleep(time.Hour)
	tg.Stop(context.Background())
}

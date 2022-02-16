package worker

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/primitive"
	"net/http"
	"testing"
	"time"
)

func Test_e2e(t *testing.T) {
	tg := NewTrigger(&primitive.Subscription{
		ID:               "test",
		Source:           "human",
		Types:            []string{"aaa"},
		Config:           map[string]string{},
		Filters:          []primitive.SubscriptionFilter{{Exact: map[string]string{"type": "none"}}},
		Sink:             "http://localhost:18080",
		Protocol:         "vanus",
		ProtocolSettings: nil,
	})
	emit := 0
	pre := 0
	go func() {
		for {
			time.Sleep(time.Second)
			cur := emit
			t.Logf("%v TPS: %d", time.Now(), cur-pre)
			pre = cur
		}
	}()
	go func() {
		for {
			event := cloudevents.NewEvent()
			event.SetID(uuid.NewString())
			event.SetSource("manual")
			event.SetType("none")
			event.SetData(cloudevents.ApplicationJSON, map[string]string{"hello": "world"})
			tg.EventArrived(context.Background(), &event)
			emit++
		}
	}()
	receive := 0
	receivePre := 0
	go http.ListenAndServe(":18080", http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receive++
		//body, err := ioutil.ReadAll(request.Body)
		//if err != nil {
		//	fmt.Println(err)
		//}
		//var _ = string(body)
	}))
	go func() {
		for {
			time.Sleep(time.Second)
			cur := receive
			t.Logf("%v RECEIVE TPS: %d", time.Now(), cur-receivePre)
			receivePre = cur
		}
	}()
	tg.Start(context.Background())

	time.Sleep(time.Hour)
	tg.Stop(context.Background())
}

package worker

import (
	"context"
	"fmt"
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/eventbus-go/pkg/inmemory"
	"io/ioutil"

	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/discovery"
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

	ebVRN := "vanus+local:eventbus:example"
	elVRN := "vanus+local:eventlog+inmemory:1?keepalive=true"
	br := &discovery.EventBusRecord{
		VRN: ebVRN,
		Logs: []*discovery.EventLogRecord{
			{
				VRN:  elVRN,
				Mode: discovery.PremWrite|discovery.PremRead,
			},
		},
	}

	ns := discovery.Find("vanus+local").(*inmemory.NameService)
	// register metadata of eventbus
	ns.Register(ebVRN, br)

	go func() {
		w, err := eb.OpenBusWriter(ebVRN)
		if err != nil {
			t.Fatal(err)
		}

		// FIXME
		time.Sleep(10 * time.Second)

		for i := 0; i < 10000000000; i++ {
			// Create an Event.
			event := ce.NewEvent()
			event.SetID(fmt.Sprintf("%d", i))
			event.SetSource("example/uri")
			event.SetType("none")
			event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world","type":"none"})

			_, err = w.Append(&event)
			if err != nil {
				t.Log(err)
			}
		}

		w.Close()
	}()
	go func() {
		ls, err := eb.LookupReadableLog(ebVRN)
		if err != nil {
			t.Fatal(err)
		}

		r, err := eb.OpenLogReader(ls[0].VRN)
		if err != nil {
			t.Fatal(err)
		}

		_, err = r.Seek(0, 0)
		if err != nil {
			t.Fatal(err)
		}

		for {
			events, err := r.Read(5)
			if err != nil {
				t.Fatal(err)
			}

			if len(events) == 0 {
				time.Sleep(time.Second)
				continue
			}

			for _, e := range events {
				tg.EventArrived(context.Background(), e)
				emit++
			}
		}

		r.Close()
	}()
	receive := 0
	receivePre := 0
	go http.ListenAndServe(":18080", http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receive++
		body, err := ioutil.ReadAll(request.Body)
		if err != nil {
			fmt.Println(err)
		}
		var _ = string(body)
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

package gateway

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/eventbus"
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	httpRequestPrefix = "/gateway"
	xceVanusEventbus  = "xvanuseventbus"
)

type ceGateway struct {
	ceClient             v2.Client
	busWriter            sync.Map
	eventbusCtrlEndpoint string
}

func NewGateway(ebCtrlEndpoint string) *ceGateway {
	return &ceGateway{
		eventbusCtrlEndpoint: ebCtrlEndpoint,
		//ceClient:             c,
	}
}

func (ga *ceGateway) StartReceive(ctx context.Context) error {
	ls, err := net.Listen("tcp4", "localhost:8080")
	if err != nil {
		return err
	}

	c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
	if err != nil {
		return err
	}
	return c.StartReceiver(ctx, ga.receive)
}

func (ga *ceGateway) receive(ctx context.Context, event v2.Event) protocol.Result {
	log.Info(fmt.Sprintf("%v", event), nil)
	ebName := getEventBusFromPath(cehttp.RequestDataFromContext(ctx))

	if ebName == "" {
		return fmt.Errorf("invalid eventbus name")
	}

	vrn := fmt.Sprintf("vanus://%s/eventbus/%s?namespace=vanus", ga.eventbusCtrlEndpoint, ebName)
	v, exist := ga.busWriter.Load(vrn)
	if !exist {
		writer, err := eb.OpenBusWriter(vrn)
		if err != nil {
			return protocol.Result(err)
		}

		var loaded bool
		v, loaded = ga.busWriter.LoadOrStore(vrn, writer)
		if loaded {
			writer.Close()
		}
	}
	event.SetExtension(xceVanusEventbus, ebName)
	writer := v.(eventbus.BusWriter)
	_, err := writer.Append(ctx, &event)
	return protocol.Result(err)
}

func getEventBusFromPath(reqData *cehttp.RequestData) string {
	// TODO validate
	reqPathStr := reqData.URL.String()
	if !strings.HasPrefix(reqPathStr, httpRequestPrefix) {
		return ""
	}
	return strings.TrimLeft(reqPathStr[len(httpRequestPrefix):], "/")
}

package event

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/linkall-labs/vanus/observability/log"

	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/vanus/client"
)

var (
	c *Config
)

func Init(cfg *Config) {
	c = cfg
}

// PutEvent
func PutEvent(ctx context.Context, eventbus string, event *ce.Event) error {
	vrn := fmt.Sprintf("vanus://%s/eventbus/%s?controllers=%s", c.Endpoints[0], eventbus, strings.Join(c.Endpoints, ","))
	writer, err := eb.OpenBusWriter(vrn)
	if err != nil {
		log.Warning(ctx, "open bus writer failed", map[string]interface{}{
			"vrn":        vrn,
			log.KeyError: err,
		})
		return err
	}

	_, err = writer.Append(ctx, event)
	if err != nil {
		log.Error(ctx, "append event to failed", map[string]interface{}{
			"event":      event,
			log.KeyError: err,
		})
		return err
	}
	log.Info(ctx, "put event success", map[string]interface{}{
		"eventbus": eventbus,
		"Time":     event.Time().Format("2006-01-02 15:04:05.000"),
	})
	return nil
}

// GetEvent
func GetEvent(ctx context.Context, eventbus string, offset int64, number int16) ([]*ce.Event, error) {
	vrn := fmt.Sprintf("vanus:///eventbus/%s?controllers=%s", eventbus, strings.Join(c.Endpoints, ","))
	ls, err := eb.LookupReadableLogs(ctx, vrn)
	if err != nil {
		log.Error(ctx, "lookup readable logs failed", map[string]interface{}{
			log.KeyError: err,
		})
		return nil, err
	}

	r, err := eb.OpenLogReader(ls[0].VRN)
	if err != nil {
		log.Error(ctx, "open log reader failed", map[string]interface{}{
			log.KeyError: err,
		})
		return nil, err
	}

	_, err = r.Seek(ctx, offset, io.SeekStart)
	if err != nil {
		log.Error(ctx, "seek failed", map[string]interface{}{
			log.KeyError: err,
		})
		return nil, err
	}

	events, err := r.Read(ctx, number)
	if err != nil {
		if err.Error() != "on end" {
			log.Error(ctx, "Read failed", map[string]interface{}{
				log.KeyError: err,
			})
		}
		return nil, err
	}
	return events, nil
}

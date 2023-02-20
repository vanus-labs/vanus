package strings

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestConvertTimezone(t *testing.T) {
	type args struct {
		source         string
		fromTimezone   string
		toTimezone     string
		targetJsonPath string
		dateTimeFormat string
	}
	tests := []struct {
		name       string
		args       args
		inputJSON  string
		wantJSON   string
		wantErr    bool
	}{
		{
			name: "Convert from UTC to CET",
			args: args{
				source:         "$.data.appinfoA",
				fromTimezone:   "UTC",
				toTimezone:     "CET",
				targetJsonPath: "$.data.appinfoB",
			},
			inputJSON: `{
				"specversion": "1.0",
				"type": "com.example.someevent",
				"source": "/mycontext",
				"subject": null,
				"id": "C234-1234-1234",
				"time": "2018-04-05T17:31:00Z",
				"comexampleextension1": "value",
				"comexampleothervalue": 5,
				"datacontenttype": "application/json",
				"data": {
					"appinfoA": "2021-08-29 12:01:10"
				}
			}`,
			wantJSON: `{
				"specversion": "1.0",
				"type": "com.example.someevent",
				"source": "/mycontext",
				"subject": null,
				"id": "C234-1234-1234",
				"time": "2018-04-05T17:31:00Z",
				"comexampleextension1": "value",
				"comexampleothervalue": 5,
				"datacontenttype": "application/json",
				"data": {
					"appinfoA": "2021-08-29 12:01:10",
					"appinfoB": "2021-08-29 10:01:10"
				}
			}`,
		},
		{
			name: "Convert from UTC to Asia/Kolkata",
			args: args{
				source:         "$.data.appinfoA",
				fromTimezone:   "UTC",
				toTimezone:     "Asia/Kolkata",
				targetJsonPath: "$.data.appinfoB",
			},
			inputJSON: `{
				"specversion": "1.0",
				"type": "com.example.someevent",
				"source": "/mycontext",
				"subject": null,
				"id": "C234-1234-1234",
				"time": "2018-04-05T17:31:00Z",
				"comexampleextension1": "value",
				"comexampleothervalue": 5,
				"datacontenttype": "application/json",
				"data": {
					"appinfoA": "2021-08-29 12:01:10"
				}
			}`,
			wantJSON: `{
				"specversion": "1.0",
				"type": "com.example.someevent",
				"source": "/mycontext",
				" subject": null,
				"id": "C234-1234-1234",
				"time": "2018-04-05T17:31:00Z",
				"comexampleextension1": "value",
				"comexampleothervalue": 5,
				"datacontenttype": "application/json",
				"data": {
					"appinfoA": "2021-08-29 12:01:10",
					"appinfoB": "2021-08-29 17:31:10"
				}
			}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input map[string]interface{}
			err := json.Unmarshal([]byte(tt.inputJSON), &input)
			if err != nil {
				t.Errorf("ConvertTimezone() error = %v", err)
				return
			}
			var want map[string]interface{}
			err = json.Unmarshal([]byte(tt.wantJSON), &want)
			if err != nil {
				t.Errorf("ConvertTimezone() error = %v", err)
				return
			}
			if err := ConvertTimezone(input, tt.args.source, tt.args.fromTimezone, tt.args.toTimezone, tt.args.targetJsonPath, tt.args.dateTimeFormat); (err != nil) != tt.wantErr {
				t.Errorf("ConvertTimezone() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(input, want) {
				t.Errorf("ConvertTimezone() got = %v, want %v", input, want)
			}
		})
	}
}

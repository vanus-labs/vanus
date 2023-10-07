package tracing

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltracer "go.opentelemetry.io/otel/trace"
)

type emptySpan string

// End completes the Span. The Span is considered complete and ready to be
// delivered through the rest of the telemetry pipeline after this method
// is called. Therefore, updates to the Span are not allowed after this
// method has been called.
func (sp emptySpan) End(_ ...oteltracer.SpanEndOption) {
	return
}

// AddEvent adds an event with the provided name and options.
func (sp emptySpan) AddEvent(_ string, _ ...oteltracer.EventOption) {
	return
}

// IsRecording returns the recording state of the Span. It will return
// true if the Span is active and events can be recorded.
func (sp emptySpan) IsRecording() bool {
	return false
}

// RecordError will record err as an exception span event for this span. An
// additional call to SetStatus is required if the Status of the Span should
// be set to Error, as this method does not change the Span status. If this
// span is not being recorded or err is nil then this method does nothing.
func (sp emptySpan) RecordError(err error, options ...oteltracer.EventOption) {
	return
}

// SpanContext returns the SpanContext of the Span. The returned SpanContext
// is usable even after the End method has been called for the Span.
func (sp emptySpan) SpanContext() oteltracer.SpanContext {
	return oteltracer.SpanContext{}
}

// SetStatus sets the status of the Span in the form of a code and a
// description, overriding previous values set. The description is only
// included in a status when the code is for an error.
func (sp emptySpan) SetStatus(_ codes.Code, _ string) {
	return
}

// SetName sets the Span name.
func (sp emptySpan) SetName(_ string) {
	return
}

// SetAttributes sets kv as attributes of the Span. If a key from kv
// already exists for an attribute of the Span it will be overwritten with
// the value contained in kv.
func (sp emptySpan) SetAttributes(_ ...attribute.KeyValue) {
	return
}

// TracerProvider returns a TracerProvider that can be used to generate
// additional Spans on the same telemetry pipeline as the current Span.
func (sp emptySpan) TracerProvider() oteltracer.TracerProvider {
	return nil
}

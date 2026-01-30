package mika

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/emillamm/mika"

// Header keys for trace context propagation
const (
	TraceParentHeader = "traceparent"
	TraceStateHeader  = "tracestate"
)

// recordHeaderCarrier adapts Kafka record headers to the OpenTelemetry TextMapCarrier interface.
type recordHeaderCarrier struct {
	record *kgo.Record
}

func newRecordHeaderCarrier(record *kgo.Record) *recordHeaderCarrier {
	return &recordHeaderCarrier{record: record}
}

func (c *recordHeaderCarrier) Get(key string) string {
	for _, h := range c.record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *recordHeaderCarrier) Set(key, value string) {
	// Check if header already exists
	for i, h := range c.record.Headers {
		if h.Key == key {
			c.record.Headers[i].Value = []byte(value)
			return
		}
	}
	// Append new header
	c.record.Headers = append(c.record.Headers, kgo.RecordHeader{
		Key:   key,
		Value: []byte(value),
	})
}

func (c *recordHeaderCarrier) Keys() []string {
	keys := make([]string, len(c.record.Headers))
	for i, h := range c.record.Headers {
		keys[i] = h.Key
	}
	return keys
}

// InjectTraceContext injects the trace context from ctx into the Kafka record headers.
// This should be called before publishing a record to propagate trace context to consumers.
func InjectTraceContext(ctx context.Context, record *kgo.Record) {
	propagator := otel.GetTextMapPropagator()
	carrier := newRecordHeaderCarrier(record)
	propagator.Inject(ctx, carrier)
}

// ExtractTraceContext extracts trace context from Kafka record headers into a new context.
// This should be called when consuming a record to continue the trace from the producer.
func ExtractTraceContext(ctx context.Context, record *kgo.Record) context.Context {
	propagator := otel.GetTextMapPropagator()
	carrier := newRecordHeaderCarrier(record)
	return propagator.Extract(ctx, carrier)
}

// StartProduceSpan starts a span for a Kafka produce operation.
// Returns the context with the span and the span itself (caller must call span.End()).
func StartProduceSpan(ctx context.Context, topic string, record *kgo.Record) (context.Context, trace.Span) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation.type", "publish"),
			attribute.Int("messaging.kafka.message.key.length", len(record.Key)),
			attribute.Int("messaging.kafka.message.value.length", len(record.Value)),
		),
	)
	return ctx, span
}

// StartConsumeSpan starts a span for a Kafka consume operation.
// It extracts trace context from the record headers to link to the producer span.
// Returns the context with the span and the span itself (caller must call span.End()).
func StartConsumeSpan(ctx context.Context, record *kgo.Record) (context.Context, trace.Span) {
	// Extract trace context from record headers
	ctx = ExtractTraceContext(ctx, record)

	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, record.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", record.Topic),
			attribute.String("messaging.operation.type", "process"),
			attribute.Int64("messaging.kafka.message.offset", record.Offset),
			attribute.Int("messaging.kafka.destination.partition", int(record.Partition)),
			attribute.Int("messaging.kafka.message.key.length", len(record.Key)),
			attribute.Int("messaging.kafka.message.value.length", len(record.Value)),
		),
	)
	return ctx, span
}

// SetSpanError marks the span as having an error.
func SetSpanError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// SetSpanOK marks the span as successful.
func SetSpanOK(span trace.Span) {
	span.SetStatus(codes.Ok, "")
}

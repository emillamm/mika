package mika

import (
	"testing"
	"github.com/twmb/franz-go/pkg/kgo"
	"strconv"
	"errors"
)

func TestHeaders(t *testing.T) {

	t.Run("create RecordHeaders from initially empty headers", func(t *testing.T) {
		h := NewHeaders(&kgo.Record{})
		if len(h.headerMap) > 0 {
			t.Errorf("headermap should be empty")
		}
	})

	t.Run("create RecordHeaders from non-empty headers", func(t *testing.T) {
		h := NewHeaders(&kgo.Record{
			Headers: []kgo.RecordHeader{
				kgo.RecordHeader{
					Key: "k1",
					Value: []byte("v1"),
				},
				kgo.RecordHeader{
					Key: "k2",
					Value: []byte("v2"),
				},
			},
		})
		if len(h.headerMap) != 2 {
			t.Errorf("headermap contain one element")
		}
		header1 := h.headerMap["k1"]
		if header1.Index != 0 || string(header1.Bytes) != "v1" {
			t.Errorf("invalid value for header1: %v", *header1)
		}
		header2 := h.headerMap["k2"]
		if header2.Index != 1 || string(header2.Bytes) != "v2" {
			t.Errorf("invalid value for header2: %v", *header2)
		}
	})

	t.Run("get and set headers", func(t *testing.T) {
		// start with initially empty record
		record := &kgo.Record{}
		h := NewHeaders(record)

		report := func(value any, err error) {
			t.Helper()
			t.Errorf("invalid value returned: %v error: %v", value, err)
		}
		// Set and get string values
		SetHeader(h, "k1", "v1", StringSerializer)
		if v, err := GetHeader(h, "k1", StringDeserializer); err != nil || v != "v1" {
			report(v, err)
		}

		// Set and get header - creating a new header from the record
		// This tests that headers are preserved even when calling NewHeaders
		SetHeader(h, "k1", "v1", StringSerializer)
		if v, err := GetHeader(NewHeaders(record), "k1", StringDeserializer); err != nil || v != "v1" {
			report(v, err)
		}

		// Set, update and get header - creating a new header from the record
		// This tests that headers are preserved even when calling NewHeaders
		SetHeader(h, "k1", "v1", StringSerializer)
		SetHeader(h, "k1", "v1a", StringSerializer)
		if v, err := GetHeader(NewHeaders(record), "k1", StringDeserializer); err != nil || v != "v1a" {
			report(v, err)
		}

		// Set and get int value
		SetHeader(h, "k2", 2, IntSerializer)
		if v, err := GetHeader(h, "k2", IntDeserializer); err != nil || v != 2 {
			report(v, err)
		}

		// GetOrSet new value
		if v, err := GetOrSetHeader(h, "k3", "v3", StringDeserializer, StringSerializer); err != nil || v != "v3" {
			report(v, err)
		}

		// GetOrSet for existing key
		if v, err := GetOrSetHeader(h, "k3", "foo", StringDeserializer, StringSerializer); err != nil || v != "v3" {
			report(v, err)
		}

		// Overwrite existing matching type
		SetHeader(h, "k1", "foo", StringSerializer)
		if v, err := GetHeader(h, "k1", StringDeserializer); err != nil || v != "foo" {
			report(v, err)
		}

		// Overwrite existing different type
		SetHeader(h, "k1", 999, IntSerializer)
		if v, err := GetHeader(h, "k1", IntDeserializer); err != nil || v != 999 {
			report(v, err)
		}

		// Get value different type
		if v, err := GetHeader(h, "k3", IntDeserializer); v != 0 || !errors.Is(err, strconv.ErrSyntax) {
			report(v, err)
		}

		// Get non-existing key
		if v, err := GetHeader(h, "k4", StringDeserializer); v != "" || !errors.Is(err, ErrHeaderDoesNotExist) {
			report(v, err)
		}

	})
}


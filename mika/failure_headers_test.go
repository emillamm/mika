package mika

import (
	"testing"
	"github.com/twmb/franz-go/pkg/kgo"
	"errors"
)

func TestFailureHeaders(t *testing.T) {

	var ErrSomeReason = errors.New("some reason")

	t.Run("init failure headers with empty headers", func(t *testing.T) {
		headers := NewHeaders(&kgo.Record{
			Topic: "topic1",
		})
		group := "group1"
		fh, err := InitFailureHeaders(headers, group, ErrSomeReason)
		if err != nil || fh.OriginalTopic != "topic1" || fh.ErrMessage != "some reason" || fh.Retries != 0 {
			t.Errorf("invalid failure header: %v, err: %v", fh, err)
		}
	})

	t.Run("init failure headers with non-empty headers", func(t *testing.T) {
		headers := NewHeaders(&kgo.Record{
			Topic: "topic1",
			Headers: []kgo.RecordHeader{
				kgo.RecordHeader{
					Key: FailureHeaderKey("org_topic", "group1"),
					Value: []byte("topic2"),
				},
				kgo.RecordHeader{
					Key: FailureHeaderKey("msg", "group1"),
					Value: []byte("reason2"),
				},
				kgo.RecordHeader{
					Key: FailureHeaderKey("retries", "group1"),
					Value: []byte("2"),
				},
			},

		})
		group := "group1"
		fh, err := InitFailureHeaders(headers, group, ErrSomeReason)
		if err != nil || fh.OriginalTopic != "topic2" || fh.ErrMessage != "reason2" || fh.Retries != 2 {
			t.Errorf("invalid failure header: %v, err: %v", fh, err)
		}
	})

	t.Run("get and set failure headers", func(t *testing.T) {
		report := func(value any, err error) {
			t.Helper()
			t.Errorf("invalid value: %v error: %v", value, err)
		}
		headers := NewHeaders(&kgo.Record{
			Topic: "topic1",
		})
		group := "group1"
		fh, err := InitFailureHeaders(headers, group, ErrSomeReason)
		if err != nil || fh.OriginalTopic != "topic1" || fh.ErrMessage != "some reason" || fh.Retries != 0 {
			t.Errorf("invalid failure header: %v, err: %v", fh, err)
		}


		if v, err := GetFailureHeader(headers, group, "retries", IntDeserializer); err != nil || v != 0 {
			report(v, err)
		}
		SetFailureHeader(headers, group, "retries", 3, IntSerializer)
		if v, err := GetFailureHeader(headers, group, "retries", IntDeserializer); err != nil || v != 3 {
			report(v, err)
		}


		if v, err := GetFailureHeader(headers, group, "org_topic", StringDeserializer); err != nil || v != "topic1" {
			report(v, err)
		}
		SetFailureHeader(headers, group, "org_topic", "foo", StringSerializer)
		if v, err := GetFailureHeader(headers, group, "org_topic", StringDeserializer); err != nil || v != "foo" {
			report(v, err)
		}
	})

	t.Run("increment retries", func(t *testing.T) {
		headers := NewHeaders(&kgo.Record{
			Topic: "topic1",
		})
		group := "group1"
		fh, err := InitFailureHeaders(headers, group, ErrSomeReason)
		if err != nil { t.Error(err) }
		validate := func(retries int) {
			t.Helper()
			if v, err := GetFailureHeader(headers, group, "retries", IntDeserializer); err != nil || v != retries || fh.Retries != retries {
				t.Errorf("invalid retries: %v, failure header: %v, error: %v", v, fh, err)
			}
		}
		validate(0)
		fh.IncrementRetries(headers, group)
		validate(1)
	})

	t.Run("failure header key", func(t *testing.T) {
		if k := FailureHeaderKey("abc", "group1"); k != "mika_err_group1_abc" {
			t.Errorf("invalid key %v", k)
		}
	})
}


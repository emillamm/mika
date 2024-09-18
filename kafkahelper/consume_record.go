package kafkahelper

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumeRecord struct {
	Underlying *kgo.Record
	ack func()
	fail func(error)
}

func NewConsumeRecord(
	underlying *kgo.Record,
	ack func(),
	fail func(error),
) *ConsumeRecord {
	return &ConsumeRecord{
		Underlying: underlying,
		ack: ack,
		fail: fail,
	}
}

func (c *ConsumeRecord) Ack() {
	c.ack()
}

func (c *ConsumeRecord) Fail(reason error) {
	c.fail(reason)
}


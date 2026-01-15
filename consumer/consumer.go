package consumer

type Consumer interface {
	Topic() string
	Retries() int
	UseDlq() bool
	Consume(record *ConsumeRecord)
}

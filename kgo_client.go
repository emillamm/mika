package mika

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

func LoadKgoClient(
	consumeTopics []string,
	group string,
	startOffset kgo.Offset,
) (client *kgo.Client, err error) {

	// TODO support other connection schemes
	opts := []kgo.Opt{
		kgo.SeedBrokers("localhost:29092"),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeResetOffset(startOffset), // Start consuming from end when there are no commits, i.e. new consumer groups
	}

	// add topics
	if len(consumeTopics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(consumeTopics...))

	}

	// add group
	if group != "" {
		opts = append(opts, kgo.ConsumerGroup(group))
	}

	client, err = kgo.NewClient(opts...)
	return
}


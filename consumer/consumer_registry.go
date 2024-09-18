package consumer

import (
	"errors"
	"sync"
	"context"
	"fmt"
)

var ErrConsumerTopicAlreadyExists = errors.New("Cannot add consumer for a topic that is already registered")
var ErrConsumerTopicDoesntExist = errors.New("Cannot fetch consumer for topic")
var ErrConsumerTopicsOutOfSync = errors.New("Consumer registry topics are out of sync with the broker")

// consumer registry is a goroutine safe implementation that keeps track of registered consumers
type ConsumerRegistry struct {
	mutex sync.Mutex
	consumers map[string]*RegisteredConsumer
}

type RegisteredConsumer struct {
	Enabled bool
	Retries int
	UseDlq bool
	Process func(*ConsumeRecord)
}


func NewConsumerRegistry() *ConsumerRegistry {
	return &ConsumerRegistry{
		consumers: make(map[string]*RegisteredConsumer),
	}
}

func (c *ConsumerRegistry) ConsumerTopics() []string {
	var topics []string
	if len(c.consumers) == 0 {
		return topics
	}
	topics = make([]string, len(c.consumers))
	i := 0
	for t := range c.consumers {
		topics[i] = t
	    	i++
    	}
	return topics
}

func (c *ConsumerRegistry) EnabledConsumerTopics() []string {
	var topics []string
	for t, v := range c.consumers {
		if v.Enabled {
			topics = append(topics, t)
		}
    	}
	return topics
}

// Adds topic and consumer to registry. If a topic already exists in the registry, returns ErrConsumerTopicAlreadyExists.
// Otherwise returns nil.
// Registered consumers are enabled by default.
func (c *ConsumerRegistry) AddConsumer(
	topic string,
	retries int,
	useDlq bool,
	process func(*ConsumeRecord),
) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.consumers[topic]; ok {
		return ErrConsumerTopicAlreadyExists
	}
	c.consumers[topic] = &RegisteredConsumer{
		Enabled: true,
		Retries: retries,
		UseDlq: useDlq,
		Process: process,
	}
	return nil
}

// Set enable/disable on consumer. Return ErrConsumerTopicDoesntExist if it doesn't exist.
// The method returns an error channel that will receive an error or nil when the sync op finishes.
// If an error is received, the broker might not be in sync with the consumer registry and it is up to
// the caller remediate by syncronizing manually.
// The provided context can be used to set a deadline for the sync op. If the context expires,
// the corresponding ctx.Err() will be sent to the error channel.
// The mutex will remain locked for as long as it takes for the sync operation to finish or for the context to expire
// which means that no changes can be made to the registry in the meantime.
func (c *ConsumerRegistry) SetEnabled(
	ctx context.Context,
	topic string,
	enabled bool,
	syncOp func()error,
) <-chan error {
	errChan := make(chan error, 1)
	wrapErr := func(err error) error { return fmt.Errorf("could not set topic %s enabled=%t: %w", topic, enabled, err) }
	if err := ctx.Err(); err != nil {
		errChan <- wrapErr(err)
		return errChan
	}
	c.mutex.Lock()
	consumer, ok := c.consumers[topic]
	if !ok {
		errChan <-wrapErr(ErrConsumerTopicDoesntExist)
		c.mutex.Unlock()
		return errChan
	}
	if consumer.Enabled == enabled {
		errChan <-nil // no-op
		c.mutex.Unlock()
		return errChan
	}
	// set enabled for topic in registry (locally)
	consumer.Enabled = enabled
	// perform sync operation (async)
	syncOpChan := make(chan error)
	go func() {
		syncOpChan <-syncOp()
	}()
	go func() {
		// Wait for sync operation to finish, fail or context expire.
		select {
		case err := <-syncOpChan:
			if err != nil {
				err = wrapErr(ErrConsumerTopicsOutOfSync)
			}
			errChan <-err
		case <-ctx.Done():
			errChan <-wrapErr(ctx.Err())
		}
		c.mutex.Unlock()
	}()
	return errChan
}

// Get consumer from topic or return ErrConsumerTopicDoesntExist if it doesn't exist.
func (c *ConsumerRegistry) GetConsumer(topic string) (*RegisteredConsumer, error) {
	consumer, ok := c.consumers[topic]
	if !ok {
		return nil, ErrConsumerTopicDoesntExist
	}
	return consumer, nil
}


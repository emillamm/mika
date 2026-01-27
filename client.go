package mika

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/emillamm/envx"
	"github.com/emillamm/mika/consumer"
	consum "github.com/emillamm/mika/consumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrClientClosed = errors.New("KafkaClient is closed")

// Exported types from consumer module
var (
	ErrConsumerTopicAlreadyExists      = consum.ErrConsumerTopicAlreadyExists
	ErrConsumerTopicDoesntExist        = consum.ErrConsumerTopicDoesntExist
	ErrDqlNotConfigured                = errors.New("This client was not configured to comsume from dlq")
	ErrRetriedRecordWithoutConsumer    = errors.New("This client does not have a consumer for a record consumed from a retry/dlq topic. This should not happen.")
	ErrRetriedRecordFromDifferentGroup = errors.New("This client group is different from the original client group that published to a retry/dlq topic")
)

type ConsumeRecord = consum.ConsumeRecord

type KafkaClient struct {
	consumerRegistry *consum.ConsumerRegistry
	consumerStatus   *consum.ConsumerStatus
	// error channel - this is never closed after the client is created
	errs         chan error
	underlying   *kgo.Client
	group        string
	retryTopic   string
	dlqTopic     string
	startedChan  chan struct{}
	doneChan     chan struct{}
	doneWaitChan chan struct{}
	startOffset  kgo.Offset
	env          envx.EnvX
}

// Create a new kafka client that will shutdown (not gracefully) if the context expires.
// In order to shutdown gracefully, i.e. finish processing and committing fetched records,
// call client.CloseGracefully(ctx) with a context that dictates the graceful shutdown period.
func NewKafkaClient(ctx context.Context, env envx.EnvX) (client *KafkaClient, err error) {
	// Initialize offset based on value of "KAFKA_CONSUMER_START_FROM"
	startOffset, err := getConsumerStartOffset(env)
	if err != nil {
		return nil, err
	}

	client = &KafkaClient{
		consumerRegistry: consum.NewConsumerRegistry(),
		consumerStatus:   consum.NewConsumerStatus(),
		errs:             make(chan error),
		startedChan:      make(chan struct{}),
		doneChan:         make(chan struct{}),
		doneWaitChan:     make(chan struct{}),
		startOffset:      startOffset,
		env:              env,
	}

	// async shutdown
	go func() {
		// wait for context to be done or for client done channel to be closed
		select {
		case <-ctx.Done():
			client.errs <- fmt.Errorf("client context cancelled/expired: %w", ctx.Err())
			client.Close()
		case <-client.doneChan:
			break
		}
		// close underlying client
		client.underlying.Close()
		// signal that wait is over and client is now closed
		client.errs <- ErrClientClosed
		close(client.doneWaitChan)
	}()

	return
}

// Register a topic and consumer. If a topic already exists in the registry, returns ErrConsumerTopicAlreadyExists,
// otherwise returns nil. Consumers must be registered before the client is started, otherwise it panics.
// A consumer is enabled by default when registered.
func (k *KafkaClient) RegisterConsumerFunc(
	topic string,
	retries int,
	useDlq bool,
	process func(*ConsumeRecord),
) error {
	if k.IsStarted() {
		panic("cannot register consumer after client has been started")
	}
	return k.consumerRegistry.AddConsumer(
		topic,
		retries,
		useDlq,
		process,
	)
}

// See RegisterConsumerFunc for behavior.
func (k *KafkaClient) RegisterConsumer(c consumer.Consumer) error {
	return k.RegisterConsumerFunc(c.Topic(), c.Retries(), c.UseDlq(), c.Consume)
}

// Set client consumer group. This must be called before client is started otherwise it will panic.
func (k *KafkaClient) SetGroup(group string) {
	if k.IsStarted() {
		panic("cannot set consumer group after client has been started")
	}
	k.group = group
}

// Set client retry topic which allows for consumers to replay failed records.
// This must be called before client is started otherwise it will panic.
// This option also requires using a consumer group. The group is used to identify
// whether or not a client "owns" a record that came from the retry topic as multiple
// clients can share the same retry topic. If a group is not configured when client.Start() is
// called, it will panic.
func (k *KafkaClient) SetRetryTopic(topic string) {
	if k.IsStarted() {
		panic("cannot set consumer group after client has been started")
	}
	if k.retryTopic != "" {
		panic("cannot set retry topic after it was already set")
	}
	k.retryTopic = topic
}

// Set client dlq topic which allows for consumers to replay failed records.
// This must be called before client is started otherwise it will panic.
// This option also requires using a consumer group. The group is used to identify
// whether or not a client "owns" a record that came from the dlq topic as multiple
// clients can share the same dlq topic. If a group is not configured when client.Start() is
// called, it will panic.
func (k *KafkaClient) SetDlqTopic(topic string) {
	if k.IsStarted() {
		panic("cannot set consumer group after client has been started")
	}
	if k.dlqTopic != "" {
		panic("cannot set dlq topic after it was already set")
	}
	k.dlqTopic = topic
}

// Setup client connection to brokers. If any consumers are registered, start consuming immediately.
// Calling this if the client is already started, will not have an effect.
// Calling Start if the client is closed, will not have an effect.
// The returned error channel is unbuffered and needs to have a listener for the full lifecycle of the client.
// Otherwise, the client will block when trying to push errors onto the channel.
func (k *KafkaClient) Start() (errs <-chan error) {
	errs = k.errs

	if k.IsStarted() {
		return
	}

	defer close(k.startedChan)

	if k.IsClosed() {
		return
	}

	// load enabled consume topcis
	consumeTopics := k.consumerRegistry.EnabledConsumerTopics()

	// add retry topic for consumption and verify that group is set
	if k.retryTopic != "" {
		if k.group == "" {
			panic("a consumer group must be set on the client if retries are enabled")
		}
		consumeTopics = append(consumeTopics, k.retryTopic)
	}

	// verify that group is set if using dlq
	if k.dlqTopic != "" && k.group == "" {
		panic("a consumer group must be set on the client if dlq is enabled")
	}

	// create underlying *kgo.Client
	underlying, err := LoadKgoClient(k.env, consumeTopics, k.group, k.startOffset)
	if err != nil {
		k.errs <- fmt.Errorf("failed to initialize *kgo.Client with error: %w", err)
		k.Close()
		return
	}
	k.underlying = underlying

	// Start consuming regardless if there are any registered enabled consumer topics.
	// If not, it will not poll.
	k.startConsuming()

	return
}

// Enable consumption of topic if it was previously disabled. This means resuming
// the consumer if it was disabled after the client was started or adding the consumer
// (for the first time) if it was disabled before the client was started.
// If the consumer is already enabled, this is a no-op.
// If topic was never registered, this will return ErrConsumerTopicDoesntExist.
// If requires remote changes, a call to SyncConsumerTopics() is made.
// If the sync operation times out (context expires), the registry might be out of sync
// with the broker. An error is returned in this case and it is up to the caller to
// handle the error by retrying the sync operation for example.
func (k *KafkaClient) EnableConsumerTopic(ctx context.Context, topic string) error {
	return <-k.consumerRegistry.SetEnabled(ctx, topic, true, k.SyncConsumerTopics)
}

// Disable consumption of topic if it was previously enabled. This means pausing
// the consumer if it was disabled after the client was started or removing the consumer
// if it was disabled before the client was started.
// If the consumer is already disabled, this is a no-op.
// If topic was never registered, this will return ErrConsumerTopicDoesntExist.
// If requires remote changes, a call to SyncConsumerTopics() is made.
// If the sync operation times out (context expires), the registry might be out of sync
// with the broker. An error is returned in this case and it is up to the caller to
// handle the error by retrying the sync operation for example.
func (k *KafkaClient) DisableConsumerTopic(ctx context.Context, topic string) error {
	return <-k.consumerRegistry.SetEnabled(ctx, topic, false, k.SyncConsumerTopics)
}

// Pause/resume consumption of topics in Kafka according to the status of the consumers in the registry.
func (k *KafkaClient) SyncConsumerTopics() error {
	// No-op if client is not yet started
	if !k.IsStarted() {
		return nil
	}
	currentPausedTopics := k.underlying.PauseFetchTopics()  // no args returns all paused topics
	currentConsumeTopics := k.underlying.GetConsumeTopics() // no args returns all paused topics
	allTopics := mapset.NewSet[string](k.consumerRegistry.ConsumerTopics()...)
	enabledTopics := mapset.NewSet[string](k.consumerRegistry.EnabledConsumerTopics()...)
	pausedTopics := mapset.NewSet[string](currentPausedTopics...)
	consumeTopics := mapset.NewSet[string](currentConsumeTopics...)
	topicsToPause := allTopics.Difference(enabledTopics).Difference(pausedTopics)   // Topics that are not enabled and not paused
	topicsToResume := enabledTopics.Intersect(pausedTopics)                         // Topics that are enabled and paused
	topicsToAdd := enabledTopics.Difference(pausedTopics).Difference(consumeTopics) // Topics that are enabled and not paused and not being consumed
	k.underlying.PauseFetchTopics(topicsToPause.ToSlice()...)
	k.underlying.AddConsumeTopics(topicsToAdd.ToSlice()...)
	k.underlying.ResumeFetchTopics(topicsToResume.ToSlice()...)
	return nil
}

// Start consuming from dlq topic by adding topic or resume
// consumption if it was previously added.
// If a dlq topic was not configured, it returns ErrDqlNotConfigured
func (k *KafkaClient) EnableDlqConsumption() error {
	if k.dlqTopic == "" {
		return ErrDqlNotConfigured
	}
	k.underlying.AddConsumeTopics(k.dlqTopic)
	k.underlying.ResumeFetchTopics(k.dlqTopic)
	return nil
}

// Stop consuming from dlq topic by pausing fetching from the topic.
// If a dlq topic was not configured, it returns ErrDqlNotConfigured.
func (k *KafkaClient) DisableDlqConsumption() error {
	if k.dlqTopic == "" {
		return ErrDqlNotConfigured
	}
	k.underlying.PauseFetchTopics(k.dlqTopic)
	return nil
}

// Produce a record to the give topic.
// If the provided context expures, the method will fail and return an error.
// If the client is closed, an error will also be returned.
// If the client is currently terminating gracefully, publishing will be allowed
// for as long as the underlying client is alive.
func (k *KafkaClient) PublishRecord(
	ctx context.Context,
	topic string,
	record *kgo.Record,
) (err error) {
	var wg sync.WaitGroup
	wg.Add(1)
	k.underlying.Produce(ctx, record, func(_ *kgo.Record, produceErr error) {
		defer wg.Done()
		if produceErr != nil {
			err = fmt.Errorf("failed to produce record: %w", produceErr)
		}
	})
	wg.Wait()
	return
}

// No-op if client is already closed. Otherwise stop consumption and close underlying client.
// When the client is fully closed, ErrClientClosed will be returned via the error channel.
func (k *KafkaClient) Close() {
	select {
	case <-k.doneChan:
		return
	default:
		k.consumerStatus.Terminate()
		close(k.doneChan)
	}
}

// Close client by stopping consumption gracefully if consoumer is actively consuming and
// not terminating. Otherwise, this will close client normally.
func (k *KafkaClient) CloseGracefylly(ctx context.Context) {
	go func() {
		k.consumerStatus.TerminateGracefully(ctx)
		select {
		case <-k.doneChan:
			k.consumerStatus.Terminate()
			return
		case <-k.consumerStatus.DoneSig():
			k.Close()
		}
	}()
}

// Returns true if client.Start() has been called, false otherwise.
func (k *KafkaClient) IsStarted() bool {
	select {
	case <-k.startedChan:
		return true
	default:
		return false
	}
}

// Returns true if the client is closed, false otherwise.
func (k *KafkaClient) IsClosed() bool {
	select {
	case <-k.doneChan:
		return true
	default:
		return false
	}
}

// Wait for client to be fully closed
func (k *KafkaClient) WaitForDone() {
	<-k.doneWaitChan
}

// Read value of KAFKA_CONSUMER_START_FROM as a timestamp.
// If the value is present, convert it to a timestamp that will be used
// as offset for new consumer groups. This will be ignored by existing consumer groups.
// If the value is not present, default to the .AtCommitted() offset, or auto.offset.reset "none"
// in Kafka. Given this configuration, new consumer groups will fail if the value is not present.
func getConsumerStartOffset(env envx.EnvX) (kgo.Offset, error) {
	startFrom, err := env.Time("KAFKA_CONSUMER_START_FROM", time.RFC3339).Value()
	if err != nil {
		if errors.Is(err, envx.ErrEmptyValue) {
			return kgo.NewOffset().AtCommitted(), nil
		}
		return kgo.Offset{}, fmt.Errorf("invalid format of KAFKA_CONSUMER_START_FROM: %w", err)
	}
	startOffset := kgo.NewOffset().AfterMilli(startFrom.UnixMilli())
	return startOffset, nil
}

// Reset consumer status and start the poll, process, commit loop.
// Calling this if the client is already consuming, will not have an effect.
// Calling this if the client is closed, will not have an effect.
func (k *KafkaClient) startConsuming() {
	if k.IsClosed() || k.consumerStatus.IsConsuming() {
		return
	}

	// initialize consumer done channel and start polling
	k.consumerStatus.Reset()
	k.consumerStatus.Start()
	k.startPollProcessCommitLoop()
}

// Start poll, process commit loop.
// It will run forever until the consumer done channel is closed.
// It will short-circuit and retry if there are no enabled consumer topics.
func (k *KafkaClient) startPollProcessCommitLoop() {
	go func() {
		for {
			if k.IsClosed() || k.consumerStatus.IsDone() {
				return
			}
			if k.consumerStatus.IsTerminating() {
				k.consumerStatus.Terminate()
				return
			}
			if len(k.consumerRegistry.EnabledConsumerTopics()) > 0 {
				k.pollProcessCommit()
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func (k *KafkaClient) pollProcessCommit() {
	// Create a context that can be passed to kgo.Client.PollFetches
	// and will be cancelled if the consumer is terminated (gracefully or not).
	// Put this in a go routine that will end once the polling finishes.
	doneChan := make(chan struct{})
	defer close(doneChan)
	fetchCtx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-k.consumerStatus.TermSig():
			cancel()
		case <-doneChan:
			break
		}
	}()

	// Block until fetches are available or context expires.
	fetches := k.underlying.PollFetches(fetchCtx)

	// If there are any errors, publish them all to the errs channel and close client without committing.
	// Only exception is if context is cancelled. because the consumer is being terminated. Then simply return.
	if fetches.Err() != nil {
		if err := fetches.Err0(); err != nil && errors.Is(err, context.Canceled) {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			k.errs <- fmt.Errorf("fetch error at topic %s, partition %v: %w", topic, partition, err)
		})
		k.Close()
		return
	}

	// This waitgroup ensures that the method blocks until all records have been processed.
	var wg sync.WaitGroup

	fetches.EachRecord(func(record *kgo.Record) {
		wg.Add(1)

		// We want headers to be created lazily and only once
		// as it requires iterating through every header value.
		getHeadersOnce := sync.OnceValue(func() *Headers { return NewHeaders(record) })
		getHeaders := func() *Headers { return getHeadersOnce() }

		// get consumer handler from topic
		consumer, err := k.getConsumerForRecord(record, getHeaders)
		if err != nil {
			// If this record came from a retry/dlq topic and the client doesn't have a consumer registered for
			// this type of record (per the FAILURE_TOPIC header), it means that something is wrong and we should
			// not call wg.Done()
			if errors.Is(err, ErrRetriedRecordWithoutConsumer) {
				wg.Done()
				return
			}
			// If this record came from a retry/dlq topic but it originated from a different client
			// (as per the FAILURE_GROUP header), simply drop the record.
			if errors.Is(err, ErrRetriedRecordFromDifferentGroup) {
				wg.Done()
				return
			}
			// this should never happen as consumers must be registered on start
			panic(fmt.Sprintf("failed to get consumer for topic %s: %v", record.Topic, err))
		}

		// if consumer is not enabled, simply drop the record
		if !consumer.Enabled {
			wg.Done()
			return
		}

		// prepare record
		var ackOnce sync.Once
		ack := func() {
			ackOnce.Do(func() {
				wg.Done()
			})
		}
		var failOnce sync.Once
		fail := func(reason error) {
			failOnce.Do(func() {
				if err := k.handleFailedRecord(record, reason, getHeaders); err != nil {
					k.errs <- fmt.Errorf("calling fail() on a record resulted in an error which can lead to blocked consumers: %w", err)
				} else {
					ack()
				}
			})
		}

		consumer.Process(consum.NewConsumeRecord(record, ack, fail))
	})
	wg.Wait()

	// Try committing offsets. If it fails, publish error and close client
	commitCtx := context.Background()
	if err := k.underlying.CommitUncommittedOffsets(commitCtx); err != nil {
		k.errs <- fmt.Errorf("failed to commit offsets - closing client: %w", err)
		k.Close()
	}
}

func (k *KafkaClient) getConsumerForRecord(record *kgo.Record, getHeaders func() *Headers) (consumer *consum.RegisteredConsumer, err error) {
	consumer, err = k.consumerRegistry.GetConsumer(record.Topic)
	// if consumer doesn't exist for topic, try getting consumer from FAILURE_TOPIC, if present.
	// This will find the right consumer if the record came from a retry/dlq topic.
	// This requires the consumer group to be defined on the client.
	if err != nil && errors.Is(err, ErrConsumerTopicDoesntExist) && k.group != "" {
		failureTopic, failureTopicErr := GetFailureHeader(getHeaders(), k.group, KeyOriginalTopic, StringDeserializer)
		if errors.Is(failureTopicErr, ErrHeaderDoesNotExist) {
			if k.isFromRetryOrDlq(record) {
				err = ErrRetriedRecordFromDifferentGroup
			}
			return
		} else if failureTopicErr != nil {
			err = fmt.Errorf("invalid header value representing original topic for group %s: %w", k.group, failureTopicErr)
		} else {
			consumer, err = k.consumerRegistry.GetConsumer(failureTopic)
			if err != nil && errors.Is(err, ErrConsumerTopicDoesntExist) {
				err = ErrRetriedRecordWithoutConsumer
			}
		}
	}
	return
}

func (k *KafkaClient) isFromRetryOrDlq(record *kgo.Record) bool {
	return (k.retryTopic != "" && record.Topic == k.retryTopic) || (k.dlqTopic != "" && record.Topic == k.dlqTopic)
}

func (k *KafkaClient) handleFailedRecord(record *kgo.Record, failureReason error, getHeaders func() *Headers) error {
	// Short-circuit if group is not defined.
	// Without a group, we cannot associate the failure headers with a consumer group.
	// We do not want to rely on a global record failure state because multiple consumers can share
	// the same retry/dlq topic. We want each consumer to have it's own state on any given record.
	if k.group == "" {
		return fmt.Errorf(
			"No consumer group for client. Therefore no Retries/dlq. Record failed with topic: %s error: %s",
			record.Topic,
			failureReason,
		)
	}

	headers := getHeaders()
	failureState, err := InitFailureHeaders(headers, k.group, failureReason)
	if err != nil {
		return err
	}

	consumer, err := k.consumerRegistry.GetConsumer(failureState.OriginalTopic)
	if err != nil {
		return fmt.Errorf("could not get consumer for topic %s: %w", failureState.OriginalTopic, err)
	}

	// publish to retry topic if one exists and there are retries left
	retriesEnabled := k.retryTopic != "" && consumer.Retries > 0
	if retriesEnabled && consumer.Retries > failureState.Retries {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second) // TODO make configurable
		failureState.IncrementRetries(headers, k.group)
		retryRecord := *record
		retryRecord.Topic = k.retryTopic
		if err := k.PublishRecord(ctx, k.retryTopic, &retryRecord); err != nil {
			return fmt.Errorf("failed to publish record to retry topic: %w", err)
		}
		return nil
	}
	dlqEnabled := k.dlqTopic != "" && k.group != "" && consumer.UseDlq
	if dlqEnabled {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second) // TODO make configurable
		dlqRecord := *record
		dlqRecord.Topic = k.dlqTopic
		if err := k.PublishRecord(ctx, k.dlqTopic, &dlqRecord); err != nil {
			return fmt.Errorf("failed to publish record to dlq topic: %w", err)
		}
		return nil
	}
	return fmt.Errorf("dlq not enabled for topic %s. Record failed with error: %s", record.Topic, failureReason)
}

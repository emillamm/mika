package kafka

import (
	"testing"
	"os"
	"math/rand"
	"context"
	"sync"
	"fmt"
	"time"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaClient(t *testing.T) {

	var client *KafkaClient
	defer func() {
		if client != nil && !client.IsClosed() {
			client.Close()
			client.WaitForDone()
		}
	}()

	t.Run("", func(t *testing.T) {
	})

	t.Run("publish to topic without retry and dql", func(t *testing.T) {

		// Initialize values
		topic := "test-topic-1"
		group := randomGroup()
		env := getEnv


		// Create client
		ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second) // client timeout
		client, err := NewKafkaClient(ctx, env)
		if err != nil { t.Fatal(err) }


		// Create record handler: Each received record is verified and processed for the given topic by calling Handle.
		// The order of received records should correspond to the order of Handle calls below.
		handler := NewRecordHandler()
		handler.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r1" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			return nil
		})
		handler.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r2" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			return nil
		})
		handler.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r3" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			return nil
		})
		ctx, _ = context.WithTimeout(ctx, 7 * time.Second) // record handler timeout
		handler.Start(ctx)


		// Configure client
		client.SetGroup(group)


		// register consumer without retry and dlq
		retries := 0
		useDlq := false
		registerConsumerWithHandler(t, client, handler, topic, retries, useDlq)


		// start client
		startClientAndHandleErrors(t, client)


		// Check that client is started
		if !client.IsStarted() {
			t.Fatal("client is not started")
		}


		// publish records
		publishRecordWithValue(ctx, t, client, "r1", topic)
		publishRecordWithValue(ctx, t, client, "r2", topic)
		publishRecordWithValue(ctx, t, client, "r3", topic)


		// Check and verify that handler consumed all and only expected records
		verifyHandlerError(t, handler)


		// Close client
		closeClientAndWait(ctx, t, client)
	})


	t.Run("publish to topic with retry and dlq", func(t *testing.T) {

		// Initialize values
		topic1WithRetry := "test-topic-1-with-retry"
		topic2WithDlq := "test-topic-2-with-dlq"
		topic3WithRetryAndDlq := "test-topic-3-with-retry-and-dlq"
		group := randomGroup()
		env := getEnv


		// Create client
		ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second) // client timeout
		client, err := NewKafkaClient(ctx, env)
		if err != nil { t.Fatal(err) }


		// Create record handler: Each received record is verified and processed for the given topic by calling Handle.
		// The order of received records should correspond to the order of Handle calls below.
		ctx, _ = context.WithTimeout(ctx, 7 * time.Second) // record handler timeout
		handleRecordAck := func(handler *RecordHandler, expectedValue string) {
			handler.Handle(func(r *ConsumeRecord) error {
				if v := string(r.Underlying.Value); v != expectedValue { return fmt.Errorf("unexpected value %v", v) }
				r.Ack()
				return nil
			})
		}
		handleRecordFail := func(handler *RecordHandler, expectedValue string) {
			handler.Handle(func(r *ConsumeRecord) error {
				if v := string(r.Underlying.Value); v != expectedValue { return fmt.Errorf("unexpected value %v", v) }
				r.Fail(ErrRecordFailed)
				return nil
			})
		}

		handler1 := NewRecordHandler() // pass through retry topic two times, then ack
		handleRecordFail(handler1, "r1")
		handleRecordFail(handler1, "r1")
		handleRecordAck(handler1, "r1")
		handler1.Start(ctx)

		handler2 := NewRecordHandler() // pass through dlq topic, then ack
		handleRecordFail(handler2, "r2")
		handleRecordAck(handler2, "r2")
		handler2.Start(ctx)

		handler3 := NewRecordHandler() // pass through retry topic two times, then through dlq topic, then ack
		handleRecordFail(handler3, "r3")
		handleRecordFail(handler3, "r3")
		handleRecordFail(handler3, "r3")
		handleRecordAck(handler3, "r3")
		handler3.Start(ctx)


		// Configure client
		client.SetGroup(group)
		client.SetRetryTopic("test-retry-topic")
		client.SetDlqTopic("test-dlq-topic")


		// register consumers for topics
		registerConsumerWithHandler(t, client, handler1, topic1WithRetry, 2, false)
		registerConsumerWithHandler(t, client, handler2, topic2WithDlq, 0, true)
		registerConsumerWithHandler(t, client, handler3, topic3WithRetryAndDlq, 2, true)


		// start client
		startClientAndHandleErrors(t, client)


		// publish records for the three topics
		publishRecordWithValue(ctx, t, client, "r1", topic1WithRetry)
		publishRecordWithValue(ctx, t, client, "r2", topic2WithDlq)
		publishRecordWithValue(ctx, t, client, "r3", topic3WithRetryAndDlq)


		// Enable dlq consumption
		if err = client.EnableDlqConsumption(); err != nil { t.Fatal(err) }


		// Check and verify that handler consumed all and only expected records
		verifyHandlerError(t, handler1)
		verifyHandlerError(t, handler2)
		verifyHandlerError(t, handler3)


		// Close client
		closeClientAndWait(ctx, t, client)
	})


	t.Run("ensure correct behavior of enabling/disabling dlq consumption", func(t *testing.T) {

		// Initialize values
		topicWithDlq := "test-topic-with-dlq"
		group := randomGroup()
		env := getEnv


		// Create client
		ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second) // client timeout
		client, err := NewKafkaClient(ctx, env)
		if err != nil { t.Fatal(err) }


		// Create record handler: Each received record is verified and processed for the given topic by calling Handle.
		// The order of received records should correspond to the order of Handle calls below.
		// The channels below will keep track of when a record is consumed by each topic.
		ctx, _ = context.WithTimeout(ctx, 17 * time.Second) // record handler timeout
		r1ReceivedChan := make(chan struct{})
		r1DlqReceivedChan := make(chan struct{})
		r2ReceivedChan := make(chan struct{})
		r2DlqReceivedChan := make(chan struct{})

		handlerDlq := NewRecordHandler()
		handlerDlq.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r1" { return fmt.Errorf("unexpected value %v", v) }
			r.Fail(ErrRecordFailed)
			close(r1ReceivedChan)
			return nil
		})
		handlerDlq.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r1" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			close(r1DlqReceivedChan)
			return nil
		})
		handlerDlq.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r2" { return fmt.Errorf("unexpected value %v", v) }
			r.Fail(ErrRecordFailed)
			close(r2ReceivedChan)
			return nil
		})
		handlerDlq.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r2" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			close(r2DlqReceivedChan)
			return nil
		})
		handlerDlq.Start(ctx)


		// Configure client
		client.SetGroup(group)
		client.SetDlqTopic("test-dlq-topic")


		// register consumers for topic
		registerConsumerWithHandler(t, client, handlerDlq, topicWithDlq, 0, true)


		// start client
		startClientAndHandleErrors(t, client)


		// Start publishing
		// Publish r1 and verify that it is received throug regular topic and not dlq.
		publishRecordWithValue(ctx, t, client, "r1", topicWithDlq)
		R1Loop:
			for {
				select {
				case <-r1DlqReceivedChan:
					t.Fatal("should not receive event r1 from dlq yet")
					return
				case <-r1ReceivedChan: // wait for r1
					break R1Loop
				}
			}
		// Enable dlq and verify that r1 is received there
		client.EnableDlqConsumption()
		<-r1DlqReceivedChan
		// Disable dlq consumption before moving on to r2
		client.DisableDlqConsumption()
		// Publish r2 and verify that it is received throug regular topic and not dlq.
		publishRecordWithValue(ctx, t, client, "r2", topicWithDlq)
		R2Loop:
			for {
				select {
				case <-r2DlqReceivedChan:
					t.Fatal("should not receive event r2 from dlq yet")
					return
				case <-r2ReceivedChan: // wait for r2
					break R2Loop
				}
			}
		// Enable dlq and verify that r2 is received there
		client.EnableDlqConsumption()
		<-r1DlqReceivedChan


		// Check and verify that handler consumed all and only expected records
		verifyHandlerError(t, handlerDlq)


		// Close client
		closeClientAndWait(ctx, t, client)
	})


	t.Run("ensure correct behavior of enabling/disabling consumers", func(t *testing.T) {

		// Initialize values
		topic1 := "test-topic-1"
		topic2 := "test-topic-2"
		group := randomGroup()
		env := getEnv


		// Create client
		ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second) // client timeout
		client, err := NewKafkaClient(ctx, env)
		if err != nil { t.Fatal(err) }


		// Create record handler: Each received record is verified and processed for the given topic by calling Handle.
		// The order of received records should correspond to the order of Handle calls below.
		// The channels below will keep track of when a record is consumed by each topic.
		ctx, _ = context.WithTimeout(ctx, 17 * time.Second) // record handler timeout
		r1Topic1ReceivedChan := make(chan struct{})
		r2Topic1ReceivedChan := make(chan struct{})
		r1Topic2ReceivedChan := make(chan struct{})
		r2Topic2ReceivedChan := make(chan struct{})

		handler1 := NewRecordHandler()
		handler1.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r1" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			close(r1Topic1ReceivedChan)
			return nil
		})
		handler1.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r2" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			close(r2Topic1ReceivedChan)
			return nil
		})
		handler1.Start(ctx)

		handler2 := NewRecordHandler()
		handler2.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r1" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			close(r1Topic2ReceivedChan)
			return nil
		})
		handler2.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r2" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			close(r2Topic2ReceivedChan)
			return nil
		})
		handler2.Start(ctx)


		// Configure client
		client.SetGroup(group)


		// register consumers for topic
		registerConsumerWithHandler(t, client, handler1, topic1, 0, false)
		registerConsumerWithHandler(t, client, handler2, topic2, 0, false)


		// Start disabling consumer for topic2
		if err = client.DisableConsumerTopic(ctx, topic2); err != nil { t.Fatal(err) }


		// start client
		startClientAndHandleErrors(t, client)


		// Start publishing
		// Publish r1 for both topics and verify that it is received only in topic1
		publishRecordWithValue(ctx, t, client, "r1", topic1)
		publishRecordWithValue(ctx, t, client, "r1", topic2)
		R1Loop:
			for {
				select {
				case <-r1Topic2ReceivedChan:
					t.Fatal("should not receive event r1 from dlq yet")
					return
				case <-r1Topic1ReceivedChan:
					break R1Loop
				}
			}
		// Enable topic2 and verify that r1 is received there
		if err = client.EnableConsumerTopic(ctx, topic2); err != nil { t.Fatal(err) }
		<-r1Topic2ReceivedChan
		// Disable topic1 before moving on to r2
		if err = client.DisableConsumerTopic(ctx, topic1); err != nil { t.Fatal(err) }
		// Publish r2 for both topics and verify that it is received only in topic2
		publishRecordWithValue(ctx, t, client, "r2", topic1)
		publishRecordWithValue(ctx, t, client, "r2", topic2)
		R2Loop:
			for {
				select {
				case <-r2Topic1ReceivedChan:
					t.Fatal("should not receive event r2 from dlq yet")
					return
				case <-r2Topic2ReceivedChan:
					break R2Loop
				}
			}
		// Enable topic1 and verify that r1 is received there
		if err = client.EnableConsumerTopic(ctx, topic1); err != nil { t.Fatal(err) }
		<-r2Topic1ReceivedChan


		// Check and verify that handlers consumed all and only expected records
		verifyHandlerError(t, handler1)
		verifyHandlerError(t, handler2)


		// Close client
		closeClientAndWait(ctx, t, client)
	})


	t.Run("Verify failure scenarios", func(t *testing.T) {

		// Initialize values
		topic := "test-topic-1"
		group := randomGroup()
		env := getEnv


		// Create client
		ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second) // client timeout
		client, err := NewKafkaClient(ctx, env)
		if err != nil { t.Fatal(err) }


		// Create record handler: Each received record is verified and processed for the given topic by calling Handle.
		// The order of received records should correspond to the order of Handle calls below.
		handler := NewRecordHandler()
		handler.Handle(func(r *ConsumeRecord) error {
			if v := string(r.Underlying.Value); v != "r1" { return fmt.Errorf("unexpected value %v", v) }
			r.Ack()
			return nil
		})
		ctx, _ = context.WithTimeout(ctx, 7 * time.Second) // record handler timeout
		handler.Start(ctx)


		// Configure client
		client.SetGroup(group)


		// verify ErrConsumerTopicAlreadyExists when registering a new consumer
		retries := 0
		useDlq := false
		registerConsumerWithHandler(t, client, handler, topic, retries, useDlq)
		err = client.RegisterConsumer(topic, retries, useDlq, func(record *ConsumeRecord) {})
		if !errors.Is(err, ErrConsumerTopicAlreadyExists) {
			t.Errorf("expected ErrConsumerTopicAlreadyExists, got %v", err)
		}


		// start client
		contextCancelledChan := make(chan struct{})
		clientClosedChan := make(chan struct{})
		errs := client.Start()
		go func() {
			for {
				select {
				case err := <-errs:
					if errors.Is(err, context.Canceled) {
						close(contextCancelledChan)
						break
					}
					if errors.Is(err, ErrClientClosed) {
						close(clientClosedChan)
						return
					}
					t.Error(err)
				}
			}
		}()
		if !client.IsStarted() { t.Fatal("client is not started") }


		// verify ErrConsumerTopicDoesntExist
		if err := client.EnableConsumerTopic(ctx, "xyz"); !errors.Is(err, ErrConsumerTopicDoesntExist) {
			t.Fatalf("expected ErrConsumerTopicDoesntExist")
		}
		if err := client.DisableConsumerTopic(ctx, "xyz"); !errors.Is(err, ErrConsumerTopicDoesntExist) {
			t.Fatalf("expected ErrConsumerTopicDoesntExist")
		}


		// verify panic when configuring client after it has been started
		expectPanic(t, func() {
			registerConsumerWithHandler(t, client, handler, "xyz", retries, useDlq)
		})
		expectPanic(t, func() {
			client.SetGroup("xyz")
		})
		expectPanic(t, func() {
			client.SetRetryTopic("xyz")
		})
		expectPanic(t, func() {
			client.SetDlqTopic("xyz")
		})


		// Check that client is started
		if !client.IsStarted() {
			t.Fatal("client is not started")
		}


		// publish records
		publishRecordWithValue(ctx, t, client, "r1", topic)


		// Check and verify that handler consumed all and only expected records
		verifyHandlerError(t, handler)


		// Stop consuming and wait for consumption to finish
		client.consumerStatus.TerminateGracefully(ctx)
		<-client.consumerStatus.DoneSig()


		// Cancel context and verify errors returned by client
		cancel()
		<-contextCancelledChan
		<-clientClosedChan
		time.Sleep(10 * time.Millisecond) // allow ErrClientClosed to arrive
		if !client.IsClosed() { t.Fatal("client is not closed") }
	})




	t.Run("same retry/dlq topic with different consumer groups (clients)", func(t *testing.T) {

		// Initialize values
		topic1 := "test-topic-1"
		topic2 := "test-topic-2"
		topic3 := "test-topic-3"


		// Create client
		ctx, _ := context.WithTimeout(context.Background(), 15 * time.Second) // client timeout
		createClient := func() *KafkaClient {
			env := getEnv
			client, err := NewKafkaClient(ctx, env)
			if err != nil { t.Fatal(err) }
			return client
		}

		group1 := randomGroup()
		group2 := randomGroup()
		group3 := randomGroup()

		client1 := createClient()
		client2 := createClient()
		client3 := createClient()


		// Create record handler: Each received record is verified and processed for the given topic by calling Handle.
		// The order of received records should correspond to the order of Handle calls below.
		ctx, _ = context.WithTimeout(ctx, 12 * time.Second) // record handler timeout
		handleRecordAck := func(handler *RecordHandler, expectedValue string) {
			handler.Handle(func(r *ConsumeRecord) error {
				if v := string(r.Underlying.Value); v != expectedValue { return fmt.Errorf("unexpected value %v", v) }
				r.Ack()
				return nil
			})
		}
		handleRecordFail := func(handler *RecordHandler, expectedValue string) {
			handler.Handle(func(r *ConsumeRecord) error {
				if v := string(r.Underlying.Value); v != expectedValue { return fmt.Errorf("unexpected value %v", v) }
				r.Fail(ErrRecordFailed)
				return nil
			})
		}

		handler1 := NewRecordHandler() // pass through retry topic two times, then ack
		handleRecordFail(handler1, "r1")
		handleRecordFail(handler1, "r1")
		handleRecordAck(handler1, "r1")
		handler1.Start(ctx)

		handler2 := NewRecordHandler() // pass through retry topic two times, then ack
		handleRecordFail(handler2, "r2")
		handleRecordFail(handler2, "r2")
		handleRecordAck(handler2, "r2")
		handler2.Start(ctx)

		handler3 := NewRecordHandler() // pass through retry topic two times, then ack
		handleRecordFail(handler3, "r3")
		handleRecordFail(handler3, "r3")
		handleRecordAck(handler3, "r3")
		handler3.Start(ctx)


		// Configure clients that share the same retry and dlq topics
		client1.SetGroup(group1)
		client1.SetRetryTopic("test-retry-topic")
		client1.SetDlqTopic("test-dlq-topic")

		client2.SetGroup(group2)
		client2.SetRetryTopic("test-retry-topic")
		client2.SetDlqTopic("test-dlq-topic")

		client3.SetGroup(group3)
		client3.SetRetryTopic("test-retry-topic")
		client3.SetDlqTopic("test-dlq-topic")


		// register consumers for topics
		registerConsumerWithHandler(t, client1, handler1, topic1, 1, true)
		registerConsumerWithHandler(t, client2, handler2, topic2, 1, true)
		registerConsumerWithHandler(t, client3, handler3, topic3, 2, true)


		// start clients
		startClientAndHandleErrors(t, client1)
		startClientAndHandleErrors(t, client2)
		startClientAndHandleErrors(t, client3)


		// publish records for the three topics
		publishRecordWithValue(ctx, t, client1, "r1", topic1)
		publishRecordWithValue(ctx, t, client2, "r2", topic2)
		publishRecordWithValue(ctx, t, client3, "r3", topic3)


		// Enable dlq consumption
		if err := client1.EnableDlqConsumption(); err != nil { t.Fatal(err) }
		if err := client2.EnableDlqConsumption(); err != nil { t.Fatal(err) }
		if err := client3.EnableDlqConsumption(); err != nil { t.Fatal(err) }


		// Check and verify that handler consumed all and only expected records
		verifyHandlerError(t, handler1)
		verifyHandlerError(t, handler2)
		verifyHandlerError(t, handler3)


		// Close clients
		closeClientAndWait(ctx, t, client1)
		closeClientAndWait(ctx, t, client2)
		closeClientAndWait(ctx, t, client3)
	})

}

var ErrRecordFailed = fmt.Errorf("simulated record failure")
type RecordHandler struct {
	handlers []func(*ConsumeRecord)error
	mutex sync.Mutex
	err error
	doneChan chan struct{}
}

func NewRecordHandler() *RecordHandler {
	v := &RecordHandler{
		doneChan: make(chan struct{}),
	}
	return v
}

func (v *RecordHandler) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				v.err = fmt.Errorf("did not receive all records before context expired: %w", ctx.Err())
				close(v.doneChan)
				return
			default:
				if v.Remaining() == 0 {
					close(v.doneChan)
					return
				}
			}
		}
	}()
}

func (v *RecordHandler) Handle(verify func(*ConsumeRecord)error) {
	v.handlers = append(v.handlers, verify)
}

// verify that the received record was expected
// NOTE if a record is received twice, this will return an error.
// In a production scenario we could expect duplicates in which case
// this would incorrectly return an error. But for testing
// we can expect to not have duplicates.
func (v *RecordHandler) Receive(record *ConsumeRecord) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	if len(v.handlers) == 0 {
		return fmt.Errorf("received unexpected record %v", record)
	}
	// get next handler
	handler := v.handlers[0]
	// remove from queue
	v.handlers = v.handlers[1:len(v.handlers)]
	return handler(record)
}

func (v *RecordHandler) IsDone() bool {
	select {
	case <-v.doneChan:
		return true
	default:
		return false
	}
}

func (v *RecordHandler) Done() <-chan struct{} {
	return v.doneChan
}

func (v *RecordHandler) Err() error {
	return v.err
}

func (v *RecordHandler) Remaining() int {
	v.mutex.Lock()
	r := len(v.handlers)
	v.mutex.Unlock()
	return r
}


func getEnv(s string) string {
	switch s {
	case "KAFKA_CONSUMER_START_FROM":
		// Ensure that we start consuming from the right timestamp
		return time.Now().Format(time.RFC3339)
	default:
		return os.Getenv(s)
	}
}

func registerConsumerWithHandler(
	t testing.TB,
	client *KafkaClient,
	handler *RecordHandler,
	topic string,
	retries int,
	useDlq bool,
) {
	t.Helper()
	err := client.RegisterConsumer(topic, retries, useDlq, func(record *ConsumeRecord) {
		if err := handler.Receive(record); err != nil {
			t.Error(err)
		}
	})
	if err != nil { t.Fatal(err) }
}

func startClientAndHandleErrors(
	t testing.TB,
	client *KafkaClient,
) {
	t.Helper()
	errs := client.Start()
	go func() {
		for {
			select {
			case err := <-errs:
				if errors.Is(err, ErrClientClosed) {
					return
				}
				t.Error(err)
			}
		}
	}()
	if !client.IsStarted() { t.Fatal("client is not started") }
}

func verifyHandlerError(t testing.TB, handler *RecordHandler) {
	t.Helper()
	select {
	case <-handler.Done():
		if err := handler.Err(); err != nil {
			t.Error(err)
		}
	}
}

func publishRecordWithValue (
	ctx context.Context,
	t testing.TB,
	client *KafkaClient,
	v string,
	topic string,
) {
	t.Helper()
	r :=  &kgo.Record{ Value: []byte(v), Topic: topic }
	if err := client.PublishRecord(ctx, topic, r); err != nil {
		t.Fatal(err)
	}
}

func closeClientAndWait(ctx context.Context, t testing.TB, client *KafkaClient) {
	client.CloseGracefylly(ctx)
	client.WaitForDone()
	time.Sleep(10 * time.Millisecond) // allow ErrClientClosed to arrive
	if !client.IsClosed() { t.Fatal("client is not closed") }
}

func expectPanic(t *testing.T, f func()) {
    t.Helper()
    defer func() { _ = recover() }()
    f()
    t.Fatalf("Expected panic")
}

// Generates group name in the form of "test_[a-z]7" e.g. test_hqbrluz
func randomGroup() string {
	chars := "abcdefghijklmnopqrstuvwxyz"
	length := 7
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return fmt.Sprintf("test_%s", string(b))
}


package consumer

import (
	"testing"
	mapset "github.com/deckarep/golang-set/v2"
	"context"
	"sync"
	"time"
	"errors"
)

func TestConsumerRegistry(t *testing.T) {

	t.Run("c", func(t *testing.T) {

		// create new consumer registry and verify no topics
		c := NewConsumerRegistry()
		verifyTopics(t, c.ConsumerTopics())
		verifyTopics(t, c.EnabledConsumerTopics())

		// add two new consumers and verify
		c.AddConsumer("topic1", 0, false, func(*ConsumeRecord) {})
		c.AddConsumer("topic2", 1, true, func(*ConsumeRecord) {})
		verifyTopics(t, c.ConsumerTopics(), "topic1", "topic2")
		verifyTopics(t, c.EnabledConsumerTopics(), "topic1", "topic2")

		// disable topic1 consumer and verify
		ctx := context.Background()
		simpleSyncOp := func()error{return nil}
		if err := <- c.SetEnabled(ctx, "topic1", false, simpleSyncOp); err != nil {
			t.Error(err)
		}
		verifyTopics(t, c.EnabledConsumerTopics(), "topic2")

		// disable topic2 consumer and let the sync operation time out (context expire)
		// and verify returned error
		var wg sync.WaitGroup
		wg.Add(1)
		longSyncOp := func()error{wg.Wait(); return nil}
		ctx, cancel := context.WithCancel(context.Background())
		returnedError := make(chan error)
		go func() {
			returnedError <-<- c.SetEnabled(ctx, "topic2", false, longSyncOp) // <-<- means send from one channel to another
		}()
		time.Sleep(10 * time.Millisecond)
		cancel()
		verifyTopics(t, c.EnabledConsumerTopics())
		if err := <-returnedError; !errors.Is(err, context.Canceled) {
			t.Errorf("expected context canceled, got: %v", err)
		}

		// enable topic1 consumer and let the sync operation return an error
		// and verify returned error
		ctx = context.Background()
		errorSyncOp := func()error{return ErrSyncFailed}
		if err := <- c.SetEnabled(ctx, "topic1", true, errorSyncOp); !errors.Is(err, ErrConsumerTopicsOutOfSync) {
			t.Errorf("expected ErrConsumerTopicsOutOfSync, got: %v", err)
		}
		verifyTopics(t, c.EnabledConsumerTopics(), "topic1")


		// enable topic2 and verify
		if err := <- c.SetEnabled(ctx, "topic2", true, simpleSyncOp); err != nil {
			t.Error(err)
		}
		verifyTopics(t, c.EnabledConsumerTopics(), "topic1", "topic2")

		// verify error when enabling a topic that doesn't exist
		if err := <- c.SetEnabled(ctx, "foo", true, simpleSyncOp); !errors.Is(err, ErrConsumerTopicDoesntExist) {
			t.Errorf("got %v, want ErrConsumerTopicDoesntExist", err)
		}

		// get consumer from topic
		t2, err := c.GetConsumer("topic2")
		if err != nil || t2.Enabled != true || t2.Retries != 1 || t2.UseDlq != true {
			t.Errorf("incorrectly configured consumer %v, error: %v", t2, err)
		}

		// get non-existing consumer from topic and verify error
		foo, err := c.GetConsumer("foo")
		if foo != nil || !errors.Is(err, ErrConsumerTopicDoesntExist) {
			t.Errorf("got %v, %v, wanted nil, ErrConsumerTopicDoesntExist", foo, err)
		}
	})

}

var ErrSyncFailed = errors.New("simulated sync failure")

func verifyTopics(
	t testing.TB,
	actual []string,
	expected ...string,
) {
	t.Helper()

	actualSet := mapset.NewSet[string](actual...)
	expectedSet := mapset.NewSet[string](expected...)
	if !actualSet.Equal(expectedSet) {
		t.Errorf("got %v, want %v", actual, expected)
	}
}


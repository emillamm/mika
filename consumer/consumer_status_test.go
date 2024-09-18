package consumer

import (
	"testing"
	"context"
	"time"
	"errors"
)

func TestConsumerStatus(t *testing.T) {

	t.Run("consumer status that transitions to done with expired context", func(t *testing.T) {

		// new consumer => no statuses set
		c := NewConsumerStatus()
		verifyStatuses(t, c, false, false, false, false)

		// calling start => started, consuming
		c.Start()
		verifyStatuses(t, c, true, true, false, false)
		ctx, cancel := context.WithCancel(context.Background())

		// calling terminate gracefully => started, consuming, terminating
		c.TerminateGracefully(ctx)
		verifyStatuses(t, c, true, true, true, false)
		cancel()
		time.Sleep(10 * time.Millisecond) // allow channels to close

		// after context expires => started, done
		verifyStatuses(t, c, true, false, false, true)
		
		// and error is not nil
		if err := c.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled but got incorrect error: %v", err)
		}
		
		// after reset => no statuses set
		c.Reset()
		verifyStatuses(t, c, false, false, false, false)
	})

	t.Run("consumer status that transitions to done with a call to terminate", func(t *testing.T) {

		// same as above
		c := NewConsumerStatus()
		c.Start()
		ctx := context.Background()
		c.TerminateGracefully(ctx)

		// calling terminate => started, done
		c.Terminate()
		time.Sleep(10 * time.Millisecond) // allow channels to close
		verifyStatuses(t, c, true, false, false, true)
		
		// and error is nil
		if err := c.Err(); err != nil {
			t.Errorf("expected no error but got: %v", err)
		}
	})
}

func verifyStatuses(
	t testing.TB,
	c *ConsumerStatus,
	started bool,
	consuming bool,
	terminating bool,
	done bool,
) {
	t.Helper()
	verify := func(expected bool, actual bool, label string) {
		t.Helper()
		if actual != expected {
			t.Errorf("invalid status %s: Expected %t, actual %t", label, expected, actual)
		}
	}
	verify(started, c.IsStarted(), "started")
	verify(consuming, c.IsConsuming(), "consuming")
	verify(terminating, c.IsTerminating(), "terminating")
	verify(done, c.IsDone(), "done")
}


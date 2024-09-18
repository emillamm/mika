package kafkahelper

import (
	"context"
	"fmt"
)

// A consumer can be in the following states
//
//  |-Started
//  |-|-Consuming
//  |-|-|-Terminating
//  |-|-Done
//
type ConsumerStatus struct {
	startedChan chan struct{}
	termChan chan struct{}
	doneChan chan struct{}
	err error
}

func NewConsumerStatus() *ConsumerStatus {
	c := &ConsumerStatus{}
	c.Reset()
	return c
}

// Return true if consumer has been started, false otherwise.
func (c *ConsumerStatus) IsStarted() bool {
	select {
	case <-c.startedChan:
		return true
	default:
		return false
	}
}

// Return true if consumer is actively consuming, i.e. is started
// and not done, otherwise false. This will also return true if client is
// in the process of terminating gracefully.
func (c *ConsumerStatus) IsConsuming() bool {
	return c.IsStarted() && !c.IsDone()
}

// Return true if consumer is currently terminating (gracefully),
// otherwise false.
func (c *ConsumerStatus) IsTerminating() bool {
	if !c.IsConsuming() {
		return false
	}
	select {
	case <- c.TermSig():
		return true
	default:
		return false
	}
}

// Return true if consumer is done, otherwise false.
func (c *ConsumerStatus) IsDone() bool {
	select {
	case <- c.DoneSig():
		return true
	default:
		return false
	}
}

// Return error if the consumer failed to process all records while shutting down gracefully, otherwise nil.
func (c *ConsumerStatus) Err() error {
	return c.err
}

// Create new status channels
func (c *ConsumerStatus) Reset() {
	c.startedChan = make(chan struct{})
	c.termChan = make(chan struct{})
	c.doneChan = make(chan struct{})
	c.err = nil
}

// Close started channel to indicate that consumer has been started.
func (c *ConsumerStatus) Start() {
	select {
	case <- c.startedChan:
		break
	default:
		close(c.startedChan)
	}
}

// Calling this will close the term sig channel to indicate that consumer should
// stop consuming. Once the consumer has finished processing records in the current fetch,
// it should call Terminate() to close the done channel and indicate upstream that no more records are in flight.
// If this is not called before the context expires, the done channel
// will be closed automatically.
func (c *ConsumerStatus) TerminateGracefully(ctx context.Context) {
	if !c.IsStarted() {
		c.Terminate()
		return
	}
	c.closeTermChan()
	go func() {
		select {
		case <- ctx.Done():
			c.err = fmt.Errorf("consumer did not finish processing records during graceful shutdown: %w", ctx.Err())
			c.Terminate()
		case <- c.DoneSig():
			break
		}

	}()
}

// When terminating gracefully, consumer should call this to indicate that it has finished processing in-flight records in order to
// close the done channel. This will be a no-op if the consumer is not currently terminating.
func (c *ConsumerStatus) Terminate() {
	c.closeTermChan()
	c.closeDoneChan()
}

// channel that indicates whether the consumer should begin graceful shutdown.
func (c *ConsumerStatus) TermSig() <-chan struct{} {
	return c.termChan
}

// channel that indicates whether the consumer is done, despite whether it finished processing
// and comitting in-flight records in time or not.
func (c *ConsumerStatus) DoneSig() <-chan struct{} {
	return c.doneChan
}

// safely close done chan if it isn't already closed
func (c *ConsumerStatus) closeDoneChan() {
	select {
	case <- c.DoneSig():
		break
	default:
		close(c.doneChan)
	}
}

// safely close term chan if it isn't already closed
func (c *ConsumerStatus) closeTermChan() {
	select {
	case <- c.TermSig():
		break
	default:
		close(c.termChan)
	}
}


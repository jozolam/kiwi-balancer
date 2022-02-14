package balancer

import (
	"context"
	"fmt"
	"time"
)

type Client interface {
	// Weight is unit-less number that determines how much processing capacity can a client be allocated
	// when running in parallel with other clients. The higher the weight, the more capacity the client receives.
	Weight() int
	// Workload returns a channel of work chunks that are ment to be processed through the Server.
	// Client's channel is always filled with work chunks.
	Workload(ctx context.Context) chan int
}

// Server defines methods required to process client's work chunks (requests).
type Server interface {
	// Process takes one work chunk (request) and does something with it. The error can be ignored.
	Process(ctx context.Context, workChunk int) error
}

// Balancer makes sure the Server is not smashed with incoming requests (work chunks) by only enabling certain number
// of parallel requests processed by the Server. Imagine there's a SLO defined, and we don't want to make the expensive
// service people angry.
//
// If implementing more advanced balancer, ake sure to correctly assign processing capacity to a client based on other
// clients currently in process.
// To give an example of this, imagine there's a maximum number of work chunks set to 100 and there are two clients
// registered, both with the same priority. When they are both served in parallel, each of them gets to send
// 50 chunks at the same time.
// In the same scenario, if there were two clients with priority 1 and one client with priority 2, the first
// two would be allowed to send 25 requests and the other one would send 50. It's likely that the one sending 50 would
// be served faster, finishing the work early, meaning that it would no longer be necessary that those first two
// clients only send 25 each but can and should use the remaining capacity and send 50 again.
type Balancer struct {
	maxLoad         int32
	clients         map[string]*clientWrapper
	queue           chan *clientWrapper
	registerQueue   chan *clientWrapper
	unregisterQueue chan bool
	eventsCount     int
	maxEventCoun    int
}

type clientWrapper struct {
	client Client
	ctx    context.Context
	load   chan int
	id     string
}

// New creates a new Balancer instance. It needs the server that it's going to balance for and a maximum number of work
// chunks that can the processor process at a time. THIS IS A HARD REQUIREMENT - THE SERVICE CANNOT PROCESS MORE THAN
// <PROVIDED NUMBER> OF WORK CHUNKS IN PARALLEL.
func New(server Server, maxLoad int32) *Balancer {
	clients := make(map[string]*clientWrapper)
	queueSize := int(maxLoad * 10)
	b := &Balancer{maxLoad: maxLoad, clients: clients, queue: make(chan *clientWrapper, queueSize), registerQueue: make(chan *clientWrapper), unregisterQueue: make(chan bool), eventsCount: 0, maxEventCoun: queueSize}

	go func() {
		for {
			if b.maxEventCoun > b.eventsCount { // this is very important condition otherwise we would create deadlock
				select {
				case cw := <-b.registerQueue: // because this is here inside a condition we are able to create backpressure on registration of new clients
					b.queue <- cw
					b.eventsCount++
				default:
				}
			}
			select {
			case <-b.unregisterQueue:
				b.eventsCount--
			default:
				time.Sleep(10 * time.Millisecond) // just to prevent empty spinning this loop
			}
		}
	}()
	for i := 0; i < int(b.maxLoad); i++ {
		index := i
		go func() {
			for {
				c := <-b.queue
				load, ok := <-c.load
				if ok == false {
					b.unregisterQueue <- true
					continue
				}
				fmt.Println("process ", index, " load ", load, " weight ", c.client.Weight())
				server.Process(c.ctx, load)
				b.queue <- c // this is kind of dirty but we made it safe by limiting total number of messages in queue in registration
			}
		}()
	}

	return b
}

// Register a client to the balancer and start processing its work chunks through provided processor (server).
// For the sake of simplicity, assume that the client has no identifier, meaning the same client can register themselves
// multiple times.
func (b *Balancer) Register(ctx context.Context, c Client) {
	w := c.Weight()
	if w > 5 { // in this algorithm is really important to limit max priority otherwise we will just pollute our buffers
		w = 5
	}
	for i := 0; i < w; i++ {
		b.registerQueue <- &clientWrapper{client: c, ctx: ctx, load: c.Workload(ctx)}
	}
}

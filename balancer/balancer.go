package balancer

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
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
	maxLoad     int32
	clients     map[string]*clientWrapper
	clientsLock *sync.Mutex
	queue       chan *clientWrapper
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
	b := &Balancer{maxLoad: maxLoad, clients: clients, clientsLock: new(sync.Mutex), queue: make(chan *clientWrapper, 2*maxLoad)}

	go func() {
		for {
			b.clientsLock.Lock()
			for _, v := range b.clients {
				for i := 0; i < v.client.Weight(); i++ {
					b.queue <- v
				}
			}

			b.clientsLock.Unlock()
		}
	}()
	for i := 0; i < int(b.maxLoad); i++ {
		index := i
		go func() {
			for {
				c := <-b.queue
				load, ok := <-c.load
				if ok == false {
					b.unregister(c)
				}
				fmt.Println("process ", index, " load ", load, " weight ", c.client.Weight())
				server.Process(c.ctx, load)
			}
		}()
	}

	return b
}

// Register a client to the balancer and start processing its work chunks through provided processor (server).
// For the sake of simplicity, assume that the client has no identifier, meaning the same client can register themselves
// multiple times.
func (b *Balancer) Register(ctx context.Context, c Client) {
	fmt.Println("registration of client")
	b.clientsLock.Lock()
	var id = generateUuid()
	for _, ok := b.clients[id]; ok == true; _, ok = b.clients[id] {
		id = generateUuid()
	}
	b.clients[id] = &clientWrapper{client: c, ctx: ctx, load: c.Workload(ctx), id: id}
	b.clientsLock.Unlock()
}

func (b *Balancer) unregister(cw *clientWrapper) {
	fmt.Println("unregistration of client")
	b.clientsLock.Lock()
	delete(b.clients, cw.id)
	b.clientsLock.Unlock()
}

func generateUuid() string {
	uuidWithHyphen := uuid.New()
	return strings.Replace(uuidWithHyphen.String(), "-", "", -1)
}

package main

import (
	"context"
	"math/rand"
	"time"

	"gitlab.com/kiwicom/search-team/balancer/balancer"
	"gitlab.com/kiwicom/search-team/balancer/client"
	"gitlab.com/kiwicom/search-team/balancer/service"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	maxParallel := int32(10)
	b := balancer.New(&service.TheExpensiveFragileService{}, maxParallel)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	nbClients := 5
	for i := 0; i < nbClients; i++ {
		go func() {
			workload := 100 + rand.Intn(100)
			weight := 1000 + rand.Intn(3)

			time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
			b.Register(ctx, client.New(workload, weight))
		}()
	}
	<-ctx.Done()
}

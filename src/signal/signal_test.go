package signal

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewSignal(t *testing.T) {
	signal := NewSignal[int](3)
	signal.Send(1)
	signal.Send(2)
	signal.Send(3)
	signal.Send(4)
	signal.Send(5)
	signal.Send(6)
	signal.Send(7)
	signal.Send(8)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-time.After(50 * time.Millisecond)
		signal.Send(9)

		<-time.After(50 * time.Millisecond)
		signal.Send(10)

		<-time.After(50 * time.Millisecond)
		signal.Send(11)

		<-time.After(50 * time.Millisecond)
		signal.Send(12)

		<-time.After(50 * time.Millisecond)
		signal.Send(13)

		<-time.After(50 * time.Millisecond)
		signal.Send(14)

		<-time.After(2 * time.Second)
		signal.Close()
	}()

	for val := range signal.ReceiveChan() {
		fmt.Println(val)
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("Done")
}

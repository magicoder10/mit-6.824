package signal

import (
	"sync"
)

type Signal[Data any] struct {
	buffer         []Data
	nextToRead     int
	nextToWrite    int
	maxBufferSize  int
	size           int
	mu             *sync.Mutex
	cond           *sync.Cond
	isClosed       bool
	waitForNewData int
}

func (s *Signal[Data]) Send(data Data) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return false
	}

	if s.size == s.maxBufferSize {
		_, ok := s.receive()
		if !ok {
			return false
		}
	}

	s.buffer[s.nextToWrite] = data
	s.size++
	s.nextToWrite = (s.nextToWrite + 1) % s.maxBufferSize

	if s.waitForNewData > 0 {
		s.waitForNewData--
		s.cond.Signal()
	}

	return true
}

func (s *Signal[Data]) Receive() (Data, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receive()
}

func (s *Signal[Data]) ReceiveChan() <-chan Data {
	ch := make(chan Data)
	go func() {
		for {
			data, ok := s.Receive()
			if !ok {
				break
			}

			ch <- data
		}

		close(ch)
	}()
	return ch
}

func (s *Signal[Data]) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

func (s *Signal[Data]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isClosed = true
	s.cond.Broadcast()
}

func (s *Signal[Data]) receive() (Data, bool) {
	if s.isClosed {
		return *new(Data), false
	}

	for s.size == 0 {
		s.waitForNewData++
		s.cond.Wait()
	}

	if s.isClosed {
		return *new(Data), false
	}

	data := s.buffer[s.nextToRead]
	s.size--
	s.nextToRead = (s.nextToRead + 1) % s.maxBufferSize
	return data, true
}

func NewSignal[Data any](maxBufferSize int) *Signal[Data] {
	mu := &sync.Mutex{}
	s := &Signal[Data]{
		buffer:        make([]Data, maxBufferSize),
		nextToRead:    0,
		nextToWrite:   0,
		maxBufferSize: maxBufferSize,
		size:          0,
		mu:            mu,
		cond:          sync.NewCond(mu),
	}
	return s
}

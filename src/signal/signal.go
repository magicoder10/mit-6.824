package signal

import (
	"sync"
)

type Signal[Data any] struct {
	buffer        []Data
	nextToRead    int
	nextToWrite   int
	maxBufferSize int
	size          int
	mu            *sync.Mutex
	cond          *sync.Cond
	isClosed      bool
}

func (s *Signal[Data]) Send(data Data) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed {
		return false
	}

	if s.size == s.maxBufferSize {
		s.receive()
	}

	s.buffer[s.nextToWrite] = data
	s.size++
	s.nextToWrite = (s.nextToWrite + 1) % s.maxBufferSize
	s.cond.Signal()

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
	s.cond.Signal()
}

func (s *Signal[Data]) receive() (Data, bool) {
	for s.size == 0 {
		if s.isClosed {
			break
		}

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

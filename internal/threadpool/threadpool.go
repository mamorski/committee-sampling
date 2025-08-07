package threadpool

import (
	"sync"
)

// Task represents a unit of work to be executed by the threadpool
type Task func()

// ThreadPool manages a pool of worker goroutines that execute tasks
type ThreadPool struct {
	workers   int
	taskQueue chan Task
	wg        sync.WaitGroup
	quit      chan bool
	once      sync.Once
}

// New creates a new ThreadPool with the specified number of workers
func New(workers int) *ThreadPool {
	if workers <= 0 {
		workers = 1
	}

	tp := &ThreadPool{
		workers:   workers,
		taskQueue: make(chan Task, workers*2), // Buffered channel for better performance
		quit:      make(chan bool),
	}
	tp.start()
	return tp
}

// start initializes and starts all worker goroutines
func (tp *ThreadPool) start() {
	for i := 0; i < tp.workers; i++ {
		go tp.worker()
	}
}

// worker is the main worker goroutine that processes tasks
func (tp *ThreadPool) worker() {
	for {
		select {
		case task := <-tp.taskQueue:
			task()
			tp.wg.Done()
		case <-tp.quit:
			return
		}
	}
}

// Submit adds a task to the threadpool for execution
func (tp *ThreadPool) Submit(task Task) {
	tp.wg.Add(1)
	tp.taskQueue <- task
}

// Wait blocks until all submitted tasks are completed
func (tp *ThreadPool) Wait() {
	tp.wg.Wait()
}

// Close shuts down the threadpool gracefully
func (tp *ThreadPool) Close() {
	tp.once.Do(func() {
		close(tp.quit)
	})
}

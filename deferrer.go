package main

import (
  "container/heap"
  "fmt"
  "time"
)

// Callback defines capability of a callback.
type Callback interface {
  Invoke()
}

// Deferrer implements scheduler for deferred functions.
type Deferrer struct {
  // number of concurrent workers to execute the callbacks
  concurrencyLevel int

  // taskInChan is a channel to submit new callbacks to dispatcher goroutine.
  taskInChan chan *task

  // workQueue is used by dispatcher to dispatch callback tasks to worker pool.
  workQueue chan *task

  // quit channel to signal dispatcher and worker goroutines to exit gracefully.
  quit chan struct{}
}

func NewDeferrer() *Deferrer {
  df := &Deferrer{
    concurrencyLevel: 5,
    quit:             make(chan struct{}),
    taskInChan:       make(chan *task),
    workQueue:        make(chan *task, 100),
  }
  // launch the dispatcher go routine.
  go df.dispatcher()

  // launch the worker pool.
  for i := 0; i < df.concurrencyLevel; i++ {
    go df.worker()
  }
  return df
}

// deferr submits given callback to be scheduled to run 'after' duration.
func (df *Deferrer) deferr(cb Callback, after time.Duration) {
  // send it to the dispatcher go routine.
  runAt := time.Now().UTC().Add(after)
  df.taskInChan <- &task{
    cb:    cb,
    runAt: runAt,
  }
}

// dispatcher implements callback scheduling functionality.
func (df *Deferrer) dispatcher() {
  // priority queue of callback tasks. Since this data structure is used only
  // inside this Goroutine in a single threaded way, we don't need any locks
  // to guard it.
  callbacks := taskQueue{}

  heap.Init(&callbacks)

  // proxy for infinite duration (an year in this case :))
  veryLongDuration := 365 * 24 * time.Hour

  // wakeUpAt refers to an event which will be fired at the earliest callback
  // task in the queue.
  wakeUpAt := time.NewTimer(veryLongDuration)

  // following inifite loop wakes up in following condition
  // * When new callback is inserted
  // * or when next callback needs to be executed
  // * or quit signal was sent.
  for {
    select {
    case task := <-df.taskInChan:
      callbacks.Push(task)
    case <-wakeUpAt.C:
      // time for next task execution
    case <-df.quit:
      return
    }
    // we are here means new task pushed or next task to be executed
    for _, tk := range callbacks.popRunnableTasks() {
      df.workQueue <- tk
    }
    // adjust the wakeUpAt accordingly
    if callbacks.Len() > 0 {
      earliestTaskAt := callbacks[0].runAt
      wakeUpAt.Reset(earliestTaskAt.Sub(time.Now()))
    } else {
      wakeUpAt.Reset(veryLongDuration)
    }
  }
}

// worker function extracts callback tasks from workQueue and executes callback.
func (df *Deferrer) worker() {
  for {
    select {
    case tk := <-df.workQueue:
      tk.cb.Invoke()
    case <-df.quit:
      return
    }
  }
}

// Stop signals Dispatcher and worker Go routines to exit.
func (df *Deferrer) Stop() {
  if df != nil {
    close(df.quit)
  }
}

// task defines a continer for callback with its execution time.
type task struct {
  cb    Callback
  runAt time.Time
}

// taskQueue defines a priority Queue data structure where task items are
// ordered by their runAt time. It uses min-heap implementation.
type taskQueue []*task

func (tq taskQueue) Len() int { return len(tq) }

func (tq taskQueue) Less(i, j int) bool {
  return tq[i].runAt.Before(tq[j].runAt)
}

func (tq taskQueue) Swap(i, j int) {
  tq[i], tq[j] = tq[j], tq[i]
}

func (tq *taskQueue) Push(x interface{}) {
  *tq = append(*tq, x.(*task))
}

func (tq *taskQueue) Pop() interface{} {
  old := *tq
  n := len(old)
  item := old[0]
  *tq = old[1:n]
  return item
}

// popRunnableTasks is a convenient function extract all the tasks which needs
// to be executed now.
func (tq *taskQueue) popRunnableTasks() []*task {
  tasks := []*task{}
  now := time.Now().UTC()
  for {
    if tq.Len() == 0 {
      return tasks
    }
    earliestTask := (*tq)[0]
    if earliestTask.runAt.Before(now) {
      popped := tq.Pop().(*task)
      tasks = append(tasks, popped)
    } else {
      return tasks
    }
  }
  return tasks
}

type CB struct{ i int }

func (cb *CB) Invoke() {
  fmt.Printf("cb: %d got invoked at:%v \n", cb.i, time.Now())
}

func main() {
  df := NewDeferrer()
  fmt.Println("scheduler is up and running")
  for i := 1; i < 10; i++ {
    df.deferr(&CB{i}, time.Duration(i)*10*time.Second)
  }
  quit := make(chan struct{})
  <-quit
}

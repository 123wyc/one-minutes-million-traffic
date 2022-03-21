package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

const (
	MaxWorker = 100 //随便设置值
	MaxQueue  = 200 // 随便设置值
)

// 一个可以发送工作请求的缓冲 channel
var JobQueue chan Job

type Payload struct{}

type Job struct {
	PayLoad Payload
}

func init() {
	JobQueue = make(chan Job, MaxQueue)
}

/**
worker
*/
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) *Worker {
	return &Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

func (worker *Worker) Start() {

	go func() {
		//worker 创建新的worker需要交给执行器去管理
		worker.WorkerPool <- worker.JobChannel
		for {
			select {
			case job := <-worker.JobChannel:
				//   真正业务的地方
				//  模拟操作耗时
				time.Sleep(500 * time.Millisecond)
				fmt.Printf("上传成功:%v\n", job)
			case <-worker.quit:
				return
			}
		}
	}()
}

//停止worker
func (worker *Worker) stop() {
	go func() {
		worker.quit <- true
	}()
}

/**
执行器
*/
type Executor struct {
	WorkerPool chan chan Job
}

//构造
func NewExecutor(maxWorkers int) *Executor {
	pool := make(chan chan Job, maxWorkers)
	return &Executor{WorkerPool: pool}
}

// 分配job给worker
func (e *Executor) dispatch() {

	for {
		select {
		//当有新任务过来时
		case job := <-JobQueue:
			go func(job Job) {
				select {
				//如果有空闲的worker
				case worker := <-e.WorkerPool:
					// 分发任务到 worker job channel 中
					worker <- job

				}
			}(job)
		}
	}
}

//启动执行器
func (e *Executor) Run() {
	//启动所有的worker
	for i := 0; i < MaxWorker; i++ {
		worker := NewWorker(e.WorkerPool)
		worker.Start()
	}
	go e.dispatch()
}

// 接收请求，把任务筛入JobQueue。
func payloadHandler(w http.ResponseWriter, r *http.Request) {
	work := Job{PayLoad: Payload{}}
	JobQueue <- work
	_, _ = w.Write([]byte("操作成功"))
}

func main() {
	// 通过调度器创建worker，监听来自 JobQueue的任务
	d := NewExecutor(MaxWorker)
	d.Run()
	http.HandleFunc("/payload", payloadHandler)
	log.Fatal(http.ListenAndServe(":8099", nil))
}
